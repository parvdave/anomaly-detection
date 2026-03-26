"""
CryptoAnomalyPipeline

Orchestrates:
  1. SparkSession creation (local mode, Kafka + PostgreSQL JARs)
  2. Reading raw trade events from Kafka as a structured stream
  3. Parsing JSON values using TRADE_SCHEMA
  4. Running anomaly detection via foreachBatch
  5. Writing results to TimescaleDB
"""

import glob
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .anomaly_detector import AnomalyDetector
from .config import AnomalyConfig, DBConfig, KafkaConfig, SparkConfig
from .schema import TRADE_SCHEMA
from .timescale_writer import TimescaleWriter

logger = logging.getLogger(__name__)


class CryptoAnomalyPipeline:
    def __init__(
        self,
        spark_cfg: SparkConfig,
        kafka_cfg: KafkaConfig,
        db_cfg: DBConfig,
        anomaly_cfg: AnomalyConfig,
    ) -> None:
        self._spark_cfg = spark_cfg
        self._kafka_cfg = kafka_cfg
        self._db_cfg = db_cfg
        self._anomaly_cfg = anomaly_cfg

    # ── Spark session ──────────────────────────────────────────────────────────

    def _build_spark_session(self) -> SparkSession:
        jars_dir = self._spark_cfg.jars_path
        jar_files = glob.glob(os.path.join(jars_dir, "*.jar"))
        jars_csv = ",".join(jar_files)
        logger.info("Loading JARs: %s", jars_csv)

        return (
            SparkSession.builder.appName(self._spark_cfg.app_name)
            .master(self._spark_cfg.master)
            .config("spark.jars", jars_csv)
            .config("spark.sql.shuffle.partitions", str(self._spark_cfg.shuffle_partitions))
            .config(
                "spark.sql.streaming.checkpointLocation",
                self._spark_cfg.checkpoint_dir,
            )
            .config("spark.ui.port", "4040")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
        )

    # ── Kafka source ───────────────────────────────────────────────────────────

    def _read_kafka_stream(self, spark: SparkSession) -> DataFrame:
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self._kafka_cfg.bootstrap_servers)
            .option("subscribe", self._kafka_cfg.topic)
            .option("startingOffsets", self._kafka_cfg.starting_offsets)
            .option("failOnDataLoss", "false")
            .option("kafka.group.id", self._kafka_cfg.group_id)
            .option("maxOffsetsPerTrigger", 10000)
            .load()
        )
        # Parse JSON value
        parsed = raw.select(
            F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("data")
        ).select("data.*")

        # Cast timestamp string → TimestampType
        parsed = parsed.withColumn(
            "event_time", F.to_timestamp(F.col("timestamp"))
        ).drop("timestamp")

        return parsed

    # ── Pipeline ───────────────────────────────────────────────────────────────

    def run(self) -> None:
        spark = self._build_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        detector = AnomalyDetector(self._anomaly_cfg)
        writer = TimescaleWriter(self._db_cfg)

        raw_stream = self._read_kafka_stream(spark)

        def process_batch(df: DataFrame, epoch_id: int) -> None:
            enriched = detector.detect(df)
            writer.get_foreachbatch_fn()(enriched, epoch_id)

        checkpoint_path = os.path.join(
            self._spark_cfg.checkpoint_dir, "crypto-anomaly"
        )

        query = (
            raw_stream.writeStream.foreachBatch(process_batch)
            .trigger(processingTime=self._spark_cfg.trigger_interval)
            .option("checkpointLocation", checkpoint_path)
            .start()
        )

        logger.info(
            "Streaming query started. Trigger: %s. Checkpoint: %s",
            self._spark_cfg.trigger_interval,
            checkpoint_path,
        )
        query.awaitTermination()
