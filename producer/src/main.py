"""
Entry point for the Binance → Kafka producer.

Starts the BinanceWebSocketClient and wires its on_message callback
to CryptoProducer.publish().
"""

import asyncio
import logging
import signal

from .binance_ws import BinanceWebSocketClient
from .producer import CryptoProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    kafka_producer = CryptoProducer()

    def on_trade(trade: dict) -> None:
        kafka_producer.publish(trade)
        logger.info("→ Kafka  %s  price=%.4f  vol=%.6f", trade["symbol"], trade["price"], trade["volume"])

    ws_client = BinanceWebSocketClient(on_message=on_trade)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(ws_client.stop()))

    try:
        await ws_client.run()
    finally:
        logger.info("Flushing Kafka producer...")
        kafka_producer.flush()
        logger.info("Producer stopped.")


if __name__ == "__main__":
    asyncio.run(main())
