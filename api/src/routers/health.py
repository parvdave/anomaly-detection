from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db
from ..models.schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check(db: AsyncSession = Depends(get_db)) -> HealthResponse:
    db_connected = False
    try:
        await db.execute(text("SELECT 1"))
        db_connected = True
        status = "healthy"
    except Exception:
        status = "degraded"

    return HealthResponse(
        status=status,
        db_connected=db_connected,
        timestamp=datetime.now(tz=timezone.utc),
    )
