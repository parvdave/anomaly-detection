"""
Tests for AnomalyDetector using PySpark in local mode.

Run with: pytest tests/test_anomaly_detector.py -v
Requires pyspark installed locally (pip install pyspark==3.5.1).
"""

import os
import sys
from datetime import datetime, timezone, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from spark.src.anomaly_detector import AnomalyDetector
from spark.src.config import AnomalyConfig


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("test-anomaly-detector")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("price", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("trade_id", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
])

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _make_rows(prices, volumes=None, symbol="BTCUSDT"):
    if volumes is None:
        volumes = [1.0] * len(prices)
    return [
        (symbol, BASE_TIME + timedelta(seconds=i), float(p), float(v), i, False)
        for i, (p, v) in enumerate(zip(prices, volumes))
    ]


def test_zscore_detects_price_spike(spark):
    cfg = AnomalyConfig(zscore_threshold=3.0, window_size=10)
    detector = AnomalyDetector(cfg)

    # 20 normal prices around 100, then a spike to 500
    prices = [100.0] * 20 + [500.0]
    rows = _make_rows(prices)
    df = spark.createDataFrame(rows, schema=SCHEMA)

    result = detector.compute_zscore_anomalies(df)
    flagged = result.filter("is_price_anomaly = true").count()
    assert flagged >= 1, "Expected at least one price anomaly for the spike"


def test_zscore_no_false_positives_on_stable_prices(spark):
    cfg = AnomalyConfig(zscore_threshold=3.0, window_size=10)
    detector = AnomalyDetector(cfg)

    prices = [100.0 + (i % 3) * 0.01 for i in range(30)]  # tiny variation
    rows = _make_rows(prices)
    df = spark.createDataFrame(rows, schema=SCHEMA)

    result = detector.compute_zscore_anomalies(df)
    flagged = result.filter("is_price_anomaly = true").count()
    assert flagged == 0, "No anomalies expected for stable prices"


def test_volume_spike_detection(spark):
    cfg = AnomalyConfig(volume_spike_multiplier=3.0, window_size=10)
    detector = AnomalyDetector(cfg)

    prices = [100.0] * 21
    volumes = [1.0] * 20 + [50.0]  # last trade is 50x avg
    rows = _make_rows(prices, volumes)
    df = spark.createDataFrame(rows, schema=SCHEMA)

    result = detector.compute_volume_spike(df)
    spikes = result.filter("is_volume_spike = true").count()
    assert spikes >= 1, "Expected at least one volume spike"


def test_detect_composite_flags(spark):
    cfg = AnomalyConfig(zscore_threshold=2.5, volume_spike_multiplier=3.0, window_size=10)
    detector = AnomalyDetector(cfg)

    prices = [100.0] * 20 + [300.0]
    volumes = [1.0] * 20 + [40.0]
    rows = _make_rows(prices, volumes)
    df = spark.createDataFrame(rows, schema=SCHEMA)

    result = detector.detect(df)
    anomaly_row = result.filter("is_anomaly = true").first()
    assert anomaly_row is not None
    assert anomaly_row["anomaly_score"] > 0
    assert anomaly_row["anomaly_type"] != ""


def test_detect_empty_dataframe(spark):
    cfg = AnomalyConfig()
    detector = AnomalyDetector(cfg)
    df = spark.createDataFrame([], schema=SCHEMA)
    result = detector.detect(df)
    assert result.count() == 0
