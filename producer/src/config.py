import os


KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
CRYPTO_TOPIC: str = os.environ.get("CRYPTO_TOPIC", "crypto.prices.raw")

# Binance WebSocket
BINANCE_WS_BASE_URL: str = os.environ.get("BINANCE_WS_URL", "wss://stream.binance.us:9443/stream")
SYMBOLS: list[str] = os.environ.get("SYMBOLS", "btcusdt,ethusdt,bnbusdt,solusdt,adausdt").split(",")
BINANCE_STREAM_TYPE: str = os.environ.get("BINANCE_STREAM_TYPE", "trade")

# Reconnect settings
MAX_RECONNECT_ATTEMPTS: int = int(os.environ.get("MAX_RECONNECT_ATTEMPTS", "5"))
RECONNECT_BASE_DELAY: float = float(os.environ.get("RECONNECT_BASE_DELAY", "2.0"))

# Proactive reconnect: re-establish before Binance's 24-hour forced disconnect
RECONNECT_AFTER_SECONDS: int = int(os.environ.get("RECONNECT_AFTER_SECONDS", str(23 * 3600)))
