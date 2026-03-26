"""
MockTradeGenerator

Generates realistic synthetic crypto trade events using Geometric Brownian Motion.
Used as an automatic fallback when Binance WebSocket is unavailable (e.g. geo-blocked).

Price model: S(t+1) = S(t) * exp(sigma * Z),  Z ~ N(0,1)
Volume model: log-normal, calibrated per symbol
Anomaly injection: every ~300 ticks per symbol a price or volume spike is injected
to keep the anomaly detection pipeline active during testing.
"""

import asyncio
import logging
import math
import random
from datetime import datetime, timezone
from typing import Callable

from . import config

logger = logging.getLogger(__name__)

# Realistic starting prices (USD) and per-tick volatility (sigma)
_SYMBOL_PARAMS: dict[str, dict] = {
    "BTCUSDT": {"price": 67000.0, "sigma": 0.0003, "vol_mean": 0.02,  "vol_std": 0.03},
    "ETHUSDT": {"price":  3500.0, "sigma": 0.0004, "vol_mean": 0.30,  "vol_std": 0.40},
    "BNBUSDT": {"price":   550.0, "sigma": 0.0004, "vol_mean": 0.50,  "vol_std": 0.60},
    "SOLUSDT": {"price":   180.0, "sigma": 0.0006, "vol_mean": 2.00,  "vol_std": 3.00},
    "ADAUSDT": {"price":     0.45,"sigma": 0.0006, "vol_mean": 500.0, "vol_std": 700.0},
}

# Fallback params for symbols not in the table above
_DEFAULT_PARAMS = {"price": 100.0, "sigma": 0.0005, "vol_mean": 1.0, "vol_std": 1.5}


class MockTradeGenerator:
    """
    Generates synthetic trade ticks that mimic the schema produced by
    BinanceWebSocketClient._normalize_trade().
    """

    def __init__(self) -> None:
        self._state: dict[str, dict] = {}
        self._tick_count: dict[str, int] = {}
        for sym in config.SYMBOLS:
            p = _SYMBOL_PARAMS.get(sym.upper(), _DEFAULT_PARAMS)
            self._state[sym] = {"price": p["price"], **p}
            self._tick_count[sym] = 0

    def next_tick(self, symbol: str) -> dict:
        s = self._state[symbol]
        self._tick_count[symbol] += 1
        n = self._tick_count[symbol]

        # GBM price step
        z = random.gauss(0, 1)
        s["price"] *= math.exp(s["sigma"] * z)

        # Log-normal volume
        log_vol = math.log(max(s["vol_mean"], 1e-12))
        volume = math.exp(random.gauss(log_vol, 0.8))

        # Inject anomaly every ~300 ticks (alternates price spike / volume spike)
        if n % 300 == 0:
            if n % 600 == 0:
                # Price spike: ±5–8 standard deviations
                direction = 1 if random.random() > 0.5 else -1
                spike = random.uniform(5, 8) * s["sigma"] * s["price"]
                s["price"] += direction * spike
                logger.debug("Injected price spike for %s: %.4f", symbol, s["price"])
            else:
                # Volume spike: 10–20x normal
                volume *= random.uniform(10, 20)
                logger.debug("Injected volume spike for %s: %.4f", symbol, volume)

        now = datetime.now(tz=timezone.utc)
        return {
            "symbol": symbol.upper(),
            "timestamp": now.isoformat(),
            "price": round(s["price"], 8),
            "volume": round(volume, 8),
            "trade_id": n,
            "is_buyer_maker": random.random() > 0.5,
        }

    async def run(self, on_message: Callable[[dict], None]) -> None:
        """
        Continuously emit synthetic ticks for all configured symbols.
        Emits ~10 ticks/second per symbol (mimics Binance trade frequency for
        mid-cap assets; BTC in reality is much higher but this is sufficient
        for anomaly detection testing).
        """
        logger.info(
            "Mock trade generator started for symbols: %s",
            ", ".join(config.SYMBOLS),
        )
        interval = 0.1  # seconds between ticks per symbol
        while True:
            for sym in config.SYMBOLS:
                tick = self.next_tick(sym)
                on_message(tick)
                logger.info(
                    "→ MOCK  %s  price=%.4f  vol=%.6f",
                    tick["symbol"], tick["price"], tick["volume"],
                )
            await asyncio.sleep(interval)
