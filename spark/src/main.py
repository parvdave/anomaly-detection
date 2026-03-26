"""Entry point for the Spark anomaly detection job."""

import logging

from .config import AnomalyConfig, DBConfig, KafkaConfig, SparkConfig
from .streaming_pipeline import CryptoAnomalyPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Starting Crypto Anomaly Detection pipeline...")

    pipeline = CryptoAnomalyPipeline(
        spark_cfg=SparkConfig(),
        kafka_cfg=KafkaConfig(),
        db_cfg=DBConfig(),
        anomaly_cfg=AnomalyConfig(),
    )

    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")


if __name__ == "__main__":
    main()
