"""
AnomalyDetector

Applies three statistical detection methods to a micro-batch DataFrame:

  1. Z-score  — rolling mean/std over the previous N rows per symbol.
               Flags abs(z) > threshold.
  2. IQR      — Q1/Q3/IQR computed per symbol within the batch via
               approxQuantile. Flags price outside [Q1 - k*IQR, Q3 + k*IQR].
  3. Volume   — rolling average volume over the previous N rows per symbol.
               Flags volume > multiplier * rolling_avg.

All three methods operate within foreachBatch on the bounded batch DataFrame.
The window looks back within the batch only; for cross-batch state, extend
with mapGroupsWithState.
"""

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .config import AnomalyConfig

logger = logging.getLogger(__name__)


class AnomalyDetector:
    def __init__(self, config: AnomalyConfig) -> None:
        self._cfg = config

    # ── Z-score ────────────────────────────────────────────────────────────────

    def compute_zscore_anomalies(self, df: DataFrame) -> DataFrame:
        """Add rolling_mean, rolling_std, zscore, is_price_anomaly columns."""
        cfg = self._cfg
        window = (
            Window.partitionBy("symbol")
            .orderBy("event_time")
            .rowsBetween(-cfg.window_size, -1)
        )
        df = df.withColumn("rolling_mean", F.avg("price").over(window))
        df = df.withColumn("rolling_std", F.stddev_pop("price").over(window))
        df = df.withColumn(
            "zscore",
            F.when(
                (F.col("rolling_std").isNotNull()) & (F.col("rolling_std") > 0),
                (F.col("price") - F.col("rolling_mean")) / F.col("rolling_std"),
            ).otherwise(F.lit(None).cast("double")),
        )
        df = df.withColumn(
            "is_price_anomaly",
            F.when(
                F.col("zscore").isNotNull()
                & (F.abs(F.col("zscore")) > cfg.zscore_threshold),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        return df

    # ── IQR ────────────────────────────────────────────────────────────────────

    def compute_iqr_anomalies(self, df: DataFrame) -> DataFrame:
        """Add is_iqr_anomaly column using per-symbol approxQuantile."""
        cfg = self._cfg
        spark: SparkSession = df.sparkSession

        symbols: list[str] = [r["symbol"] for r in df.select("symbol").distinct().collect()]

        rows: list[dict] = []
        for sym in symbols:
            quantiles = (
                df.filter(F.col("symbol") == sym)
                .approxQuantile("price", [0.25, 0.75], 0.01)
            )
            if len(quantiles) == 2:
                q1, q3 = quantiles
                iqr = q3 - q1
                lower = q1 - cfg.iqr_multiplier * iqr
                upper = q3 + cfg.iqr_multiplier * iqr
            else:
                lower, upper = float("-inf"), float("inf")
            rows.append({"symbol": sym, "iqr_lower": lower, "iqr_upper": upper})

        from pyspark.sql.types import DoubleType, StringType, StructField, StructType

        bounds_schema = StructType(
            [
                StructField("symbol", StringType(), False),
                StructField("iqr_lower", DoubleType(), True),
                StructField("iqr_upper", DoubleType(), True),
            ]
        )
        bounds_df = spark.createDataFrame(rows, schema=bounds_schema)
        df = df.join(bounds_df, on="symbol", how="left")
        df = df.withColumn(
            "is_iqr_anomaly",
            F.when(
                F.col("iqr_lower").isNotNull()
                & (
                    (F.col("price") < F.col("iqr_lower"))
                    | (F.col("price") > F.col("iqr_upper"))
                ),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        df = df.drop("iqr_lower", "iqr_upper")
        return df

    # ── Volume spike ───────────────────────────────────────────────────────────

    def compute_volume_spike(self, df: DataFrame) -> DataFrame:
        """Add rolling_avg_volume, is_volume_spike columns."""
        cfg = self._cfg
        window = (
            Window.partitionBy("symbol")
            .orderBy("event_time")
            .rowsBetween(-cfg.window_size, -1)
        )
        df = df.withColumn("rolling_avg_volume", F.avg("volume").over(window))
        df = df.withColumn(
            "is_volume_spike",
            F.when(
                F.col("rolling_avg_volume").isNotNull()
                & (F.col("rolling_avg_volume") > 0)
                & (F.col("volume") > cfg.volume_spike_multiplier * F.col("rolling_avg_volume")),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        return df

    # ── Combined ───────────────────────────────────────────────────────────────

    def detect(self, df: DataFrame) -> DataFrame:
        """
        Run all three detectors and add composite columns:
          - is_anomaly        BOOLEAN
          - anomaly_type      STRING  (pipe-separated list of triggered types)
          - anomaly_score     DOUBLE  (max absolute deviation metric)
        """
        if df.isEmpty():
            return df

        df = self.compute_zscore_anomalies(df)
        df = self.compute_iqr_anomalies(df)
        df = self.compute_volume_spike(df)

        # Composite flag
        df = df.withColumn(
            "is_anomaly",
            F.col("is_price_anomaly") | F.col("is_iqr_anomaly") | F.col("is_volume_spike"),
        )

        # Anomaly type string
        df = df.withColumn(
            "anomaly_type",
            F.concat_ws(
                "|",
                F.when(F.col("is_price_anomaly"), F.lit("PRICE_ZSCORE")),
                F.when(F.col("is_iqr_anomaly"), F.lit("PRICE_IQR")),
                F.when(F.col("is_volume_spike"), F.lit("VOLUME_SPIKE")),
            ),
        )

        # Anomaly score: max of |z-score| and volume ratio
        volume_ratio = F.when(
            F.col("rolling_avg_volume").isNotNull() & (F.col("rolling_avg_volume") > 0),
            F.col("volume") / F.col("rolling_avg_volume"),
        ).otherwise(F.lit(0.0))

        df = df.withColumn(
            "anomaly_score",
            F.greatest(
                F.coalesce(F.abs(F.col("zscore")), F.lit(0.0)),
                volume_ratio,
            ),
        )

        return df
