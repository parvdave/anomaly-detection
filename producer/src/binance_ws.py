"""
BinanceWebSocketClient

Connects to the Binance combined @trade stream for multiple symbols.
Handles:
  - Ping/pong frames (required by Binance protocol)
  - Exponential backoff reconnection on failure
  - Proactive reconnect before Binance's 24-hour forced disconnect
"""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Callable

import websockets
from websockets.exceptions import ConnectionClosed

from . import config

logger = logging.getLogger(__name__)


def _build_stream_url(symbols: list[str], stream_type: str) -> str:
    streams = "/".join(f"{s.lower()}@{stream_type}" for s in symbols)
    return f"{config.BINANCE_WS_BASE_URL}?streams={streams}"


def _normalize_trade(raw: dict) -> dict:
    """Map Binance @trade event fields to our internal schema."""
    trade_time_ms: int = raw["T"]
    event_time = datetime.fromtimestamp(trade_time_ms / 1000.0, tz=timezone.utc)
    return {
        "symbol": raw["s"],                       # e.g. "BTCUSDT"
        "timestamp": event_time.isoformat(),       # ISO8601 UTC
        "price": float(raw["p"]),
        "volume": float(raw["q"]),                 # base-asset quantity per trade
        "trade_id": int(raw["t"]),
        "is_buyer_maker": bool(raw["m"]),
    }


class BinanceWebSocketClient:
    """
    Asynchronous Binance WebSocket client for real-time trade data.

    Usage:
        client = BinanceWebSocketClient(on_message=my_callback)
        await client.run()
    """

    def __init__(self, on_message: Callable[[dict], None]) -> None:
        self._on_message = on_message
        self._url = _build_stream_url(config.SYMBOLS, config.BINANCE_STREAM_TYPE)
        self._running = False

    async def run(self) -> None:
        """Main loop with exponential backoff reconnection."""
        self._running = True
        attempt = 0

        while self._running:
            delay = config.RECONNECT_BASE_DELAY * (2 ** min(attempt, 4))
            if attempt > 0:
                logger.info("Reconnecting in %.1fs (attempt %d)...", delay, attempt)
                await asyncio.sleep(delay)

            if attempt >= config.MAX_RECONNECT_ATTEMPTS:
                logger.error("Max reconnect attempts (%d) reached. Stopping.", config.MAX_RECONNECT_ATTEMPTS)
                break

            try:
                await self._connect_and_consume()
                attempt = 0  # reset on clean exit (proactive reconnect)
            except ConnectionClosed as exc:
                logger.warning("WebSocket closed: %s", exc)
                attempt += 1
            except Exception as exc:
                logger.error("Unexpected error: %s", exc, exc_info=True)
                attempt += 1

    async def stop(self) -> None:
        self._running = False

    async def _connect_and_consume(self) -> None:
        """
        Open a single WebSocket connection and consume messages until either:
          - The connection drops (raises ConnectionClosed)
          - RECONNECT_AFTER_SECONDS elapses (proactive reconnect)
        """
        logger.info("Connecting to Binance WebSocket: %s", self._url)
        connect_time = time.monotonic()

        async with websockets.connect(
            self._url,
            ping_interval=20,   # send ping every 20s
            ping_timeout=10,    # wait 10s for pong before closing
            close_timeout=5,
        ) as ws:
            logger.info("Connected. Streaming %d symbols.", len(config.SYMBOLS))

            async for raw_message in ws:
                # Check if we should proactively reconnect (23-hour limit)
                if time.monotonic() - connect_time >= config.RECONNECT_AFTER_SECONDS:
                    logger.info("23-hour limit reached, proactively reconnecting.")
                    return  # exits cleanly → outer loop reconnects with attempt=0

                envelope = json.loads(raw_message)
                # Combined stream format: {"stream": "btcusdt@trade", "data": {...}}
                data = envelope.get("data", envelope)
                if data.get("e") != "trade":
                    continue

                try:
                    trade = _normalize_trade(data)
                    self._on_message(trade)
                except (KeyError, ValueError) as exc:
                    logger.warning("Failed to parse trade message: %s — %s", exc, data)
