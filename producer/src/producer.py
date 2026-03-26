"""
CryptoProducer

Wraps a confluent-kafka Producer. Serialises trade dicts to JSON and
publishes them to Kafka, keyed by symbol for partition affinity.
"""

import json
import logging

from confluent_kafka import Producer, KafkaException

from . import config

logger = logging.getLogger(__name__)


class CryptoProducer:
    def __init__(self) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 500,
                "linger.ms": 10,
                "batch.size": 16384,
            }
        )
        self._topic = config.CRYPTO_TOPIC

    def publish(self, trade: dict) -> None:
        key = trade["symbol"].encode("utf-8")
        value = json.dumps(trade).encode("utf-8")
        self._producer.produce(
            topic=self._topic,
            key=key,
            value=value,
            on_delivery=self._delivery_callback,
        )
        # Poll to trigger delivery callbacks without blocking
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()

    @staticmethod
    def _delivery_callback(err, msg) -> None:
        if err:
            logger.error(
                "Delivery failed for topic=%s partition=%s: %s",
                msg.topic(), msg.partition(), err,
            )
        else:
            logger.debug(
                "Delivered to %s [%d] @ offset %d",
                msg.topic(), msg.partition(), msg.offset(),
            )
