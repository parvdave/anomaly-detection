from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db
from ..models.schemas import AnomalyEventResponse, AnomalyStatsResponse
from ..services.anomaly_service import AnomalyService

router = APIRouter(prefix="/anomalies", tags=["anomalies"])


@router.get("/", response_model=list[AnomalyEventResponse])
async def list_anomalies(
    symbol: Optional[str] = Query(None, description="Filter by symbol, e.g. BTCUSDT"),
    anomaly_type: Optional[str] = Query(None, description="Substring match on anomaly_type"),
    min_score: Optional[float] = Query(None, ge=0.0, description="Minimum anomaly score"),
    start_time: Optional[datetime] = Query(None, description="ISO8601 start time (inclusive)"),
    end_time: Optional[datetime] = Query(None, description="ISO8601 end time (inclusive)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> list[AnomalyEventResponse]:
    svc = AnomalyService(db)
    events = await svc.get_anomalies(
        symbol=symbol,
        anomaly_type=anomaly_type,
        min_score=min_score,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        offset=offset,
    )
    return [AnomalyEventResponse.model_validate(e) for e in events]


@router.get("/stats", response_model=AnomalyStatsResponse)
async def anomaly_stats(db: AsyncSession = Depends(get_db)) -> AnomalyStatsResponse:
    svc = AnomalyService(db)
    data = await svc.get_stats()
    return AnomalyStatsResponse(**data)


@router.get("/{anomaly_id}", response_model=AnomalyEventResponse)
async def get_anomaly(
    anomaly_id: int,
    db: AsyncSession = Depends(get_db),
) -> AnomalyEventResponse:
    svc = AnomalyService(db)
    event = await svc.get_anomaly_by_id(anomaly_id)
    if not event:
        raise HTTPException(status_code=404, detail="Anomaly not found")
    return AnomalyEventResponse.model_validate(event)
