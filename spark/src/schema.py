from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Schema of the JSON value written by the producer to Kafka.
# Matches the dict returned by binance_ws._normalize_trade().
TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),   # ISO8601 UTC string
        StructField("price", DoubleType(), nullable=False),
        StructField("volume", DoubleType(), nullable=False),      # base-asset quantity
        StructField("trade_id", LongType(), nullable=True),
        StructField("is_buyer_maker", BooleanType(), nullable=True),
    ]
)
