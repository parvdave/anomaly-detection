"""
Entry point for the Binance → Kafka producer.

Starts the BinanceWebSocketClient and wires its on_message callback
to CryptoProducer.publish().
"""

import asyncio
import logging
import signal

from .binance_ws import BinanceUnavailableError, BinanceWebSocketClient
from .data_generator import MockTradeGenerator
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

    loop = asyncio.get_running_loop()

    try:
        # ── Attempt live Binance stream ────────────────────────────────────────
        logger.info("Attempting Binance WebSocket connection...")
        ws_client = BinanceWebSocketClient(on_message=on_trade)
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(ws_client.stop()))
        await ws_client.run()

    except BinanceUnavailableError as exc:
        # ── Fallback: mock generator ───────────────────────────────────────────
        logger.warning("%s", exc)
        logger.info("Falling back to mock trade generator.")
        mock = MockTradeGenerator()
        stop_event = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        mock_task = asyncio.create_task(mock.run(on_trade))
        await stop_event.wait()
        mock_task.cancel()

    finally:
        logger.info("Flushing Kafka producer...")
        kafka_producer.flush()
        logger.info("Producer stopped.")


if __name__ == "__main__":
    asyncio.run(main())
