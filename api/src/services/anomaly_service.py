from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.db import AnomalyEvent


class AnomalyService:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def get_anomalies(
        self,
        symbol: Optional[str] = None,
        anomaly_type: Optional[str] = None,
        min_score: Optional[float] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[AnomalyEvent]:
        stmt = select(AnomalyEvent)

        if symbol:
            stmt = stmt.where(AnomalyEvent.symbol == symbol)
        if anomaly_type:
            stmt = stmt.where(AnomalyEvent.anomaly_type.ilike(f"%{anomaly_type}%"))
        if min_score is not None:
            stmt = stmt.where(AnomalyEvent.anomaly_score >= min_score)
        if start_time:
            stmt = stmt.where(AnomalyEvent.event_time >= start_time)
        if end_time:
            stmt = stmt.where(AnomalyEvent.event_time <= end_time)

        stmt = stmt.order_by(AnomalyEvent.event_time.desc()).limit(limit).offset(offset)
        result = await self._db.execute(stmt)
        return list(result.scalars().all())

    async def get_anomaly_by_id(self, anomaly_id: int) -> Optional[AnomalyEvent]:
        result = await self._db.execute(
            select(AnomalyEvent).where(AnomalyEvent.id == anomaly_id)
        )
        return result.scalar_one_or_none()

    async def get_stats(self) -> dict:
        from datetime import timedelta

        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=24)

        # Total count
        total_result = await self._db.execute(
            select(func.count(AnomalyEvent.id)).where(AnomalyEvent.event_time >= cutoff)
        )
        total = total_result.scalar_one()

        # By symbol
        sym_result = await self._db.execute(
            select(AnomalyEvent.symbol, func.count(AnomalyEvent.id))
            .where(AnomalyEvent.event_time >= cutoff)
            .group_by(AnomalyEvent.symbol)
        )
        by_symbol = {row[0]: row[1] for row in sym_result.all()}

        # By type (exact match on stored pipe-separated value)
        type_result = await self._db.execute(
            select(AnomalyEvent.anomaly_type, func.count(AnomalyEvent.id))
            .where(AnomalyEvent.event_time >= cutoff)
            .group_by(AnomalyEvent.anomaly_type)
        )
        by_type = {row[0]: row[1] for row in type_result.all()}

        return {
            "total_last_24h": total,
            "by_symbol": by_symbol,
            "by_type": by_type,
        }
