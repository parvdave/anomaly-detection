"""Tests for BinanceWebSocketClient message normalisation."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from producer.src.binance_ws import _normalize_trade, _build_stream_url


SAMPLE_TRADE = {
    "e": "trade",
    "E": 1711440000000,
    "s": "BTCUSDT",
    "t": 3456789,
    "p": "67432.15",
    "q": "0.00123",
    "T": 1711440000000,
    "m": False,
    "M": True,
}


def test_normalize_trade_fields():
    result = _normalize_trade(SAMPLE_TRADE)
    assert result["symbol"] == "BTCUSDT"
    assert result["price"] == 67432.15
    assert result["volume"] == 0.00123
    assert result["trade_id"] == 3456789
    assert result["is_buyer_maker"] is False
    assert "timestamp" in result
    assert result["timestamp"].endswith("+00:00") or result["timestamp"].endswith("Z") or "T" in result["timestamp"]


def test_normalize_trade_price_is_float():
    result = _normalize_trade(SAMPLE_TRADE)
    assert isinstance(result["price"], float)
    assert isinstance(result["volume"], float)


def test_build_stream_url_contains_all_symbols():
    symbols = ["btcusdt", "ethusdt", "bnbusdt"]
    url = _build_stream_url(symbols, "trade")
    for sym in symbols:
        assert f"{sym}@trade" in url
    assert url.startswith("wss://")


def test_build_stream_url_combined_format():
    url = _build_stream_url(["btcusdt", "ethusdt"], "trade")
    assert "streams=" in url
    assert "btcusdt@trade/ethusdt@trade" in url
