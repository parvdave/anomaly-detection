"""
TimescaleWriter

Provides a foreachBatch sink function that writes micro-batch DataFrames
to TimescaleDB via JDBC:
  - All rows → raw_prices
  - Rows where is_anomaly=True → anomaly_events
"""

import logging

from pyspark.sql import DataFrame

from .config import DBConfig

logger = logging.getLogger(__name__)


class TimescaleWriter:
    def __init__(self, db_config: DBConfig) -> None:
        self._cfg = db_config

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _jdbc_write(self, df: DataFrame, table: str) -> None:
        (
            df.write.format("jdbc")
            .option("url", self._cfg.jdbc_url)
            .option("dbtable", table)
            .option("user", self._cfg.user)
            .option("password", self._cfg.password)
            .option("driver", self._cfg.driver)
            .option("batchsize", 1000)
            .mode("append")
            .save()
        )

    def write_raw_prices(self, df: DataFrame) -> None:
        raw = df.select(
            "symbol",
            "event_time",
            "price",
            "volume",
            "trade_id",
            "is_buyer_maker",
        )
        count = raw.count()
        if count == 0:
            return
        logger.info("Writing %d rows to raw_prices", count)
        self._jdbc_write(raw, "raw_prices")

    def write_anomalies(self, df: DataFrame) -> None:
        anomalies = df.filter("is_anomaly = true").select(
            "symbol",
            "event_time",
            "price",
            "volume",
            "anomaly_type",
            "anomaly_score",
            "zscore",
            "rolling_mean",
            "rolling_std",
            "is_price_anomaly",
            "is_iqr_anomaly",
            "is_volume_spike",
        )
        count = anomalies.count()
        if count == 0:
            return
        logger.info("Writing %d anomaly events", count)
        self._jdbc_write(anomalies, "anomaly_events")

    # ── Public foreachBatch callable ───────────────────────────────────────────

    def get_foreachbatch_fn(self):
        """Return a closure compatible with writeStream.foreachBatch()."""

        writer = self  # capture self for closure

        def process_batch(df: DataFrame, epoch_id: int) -> None:
            logger.info("Processing batch epoch_id=%d", epoch_id)
            if df.isEmpty():
                logger.debug("Empty batch, skipping.")
                return
            # Cache to avoid re-computation for the two writes
            df.cache()
            try:
                writer.write_raw_prices(df)
                writer.write_anomalies(df)
            finally:
                df.unpersist()

        return process_batch
