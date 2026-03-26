from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Double, String, text
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base


class RawPrice(Base):
    __tablename__ = "raw_prices"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    price: Mapped[float] = mapped_column(Double, nullable=False)
    volume: Mapped[float] = mapped_column(Double, nullable=False)
    trade_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    is_buyer_maker: Mapped[Optional[bool]] = mapped_column(Boolean)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )


class AnomalyEvent(Base):
    __tablename__ = "anomaly_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    price: Mapped[float] = mapped_column(Double, nullable=False)
    volume: Mapped[float] = mapped_column(Double, nullable=False)
    anomaly_type: Mapped[str] = mapped_column(String(100), nullable=False)
    anomaly_score: Mapped[float] = mapped_column(Double, nullable=False)
    zscore: Mapped[Optional[float]] = mapped_column(Double)
    rolling_mean: Mapped[Optional[float]] = mapped_column(Double)
    rolling_std: Mapped[Optional[float]] = mapped_column(Double)
    is_price_anomaly: Mapped[bool] = mapped_column(Boolean, default=False)
    is_iqr_anomaly: Mapped[bool] = mapped_column(Boolean, default=False)
    is_volume_spike: Mapped[bool] = mapped_column(Boolean, default=False)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )
