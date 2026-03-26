from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class AnomalyEventResponse(BaseModel):
    id: int
    symbol: str
    event_time: datetime
    price: float
    volume: float
    anomaly_type: str
    anomaly_score: float
    zscore: Optional[float] = None
    rolling_mean: Optional[float] = None
    rolling_std: Optional[float] = None
    is_price_anomaly: bool
    is_iqr_anomaly: bool
    is_volume_spike: bool
    detected_at: datetime

    model_config = ConfigDict(from_attributes=True)


class AnomalyStatsResponse(BaseModel):
    total_last_24h: int
    by_symbol: dict[str, int]
    by_type: dict[str, int]


class HealthResponse(BaseModel):
    status: str  # "healthy" | "degraded"
    db_connected: bool
    timestamp: datetime
    version: str = "1.0.0"
