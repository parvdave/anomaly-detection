"""
FastAPI application factory.

Lifespan:
  - Startup: initialise DB engine, start AlertBroadcaster polling task.
  - Shutdown: stop polling, dispose DB engine.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .db import AsyncSessionLocal, engine
from .routers import anomalies, health, websocket
from .services.alert_broadcaster import broadcaster

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up API...")
    broadcaster.start_polling(AsyncSessionLocal)
    yield
    logger.info("Shutting down API...")
    broadcaster.stop_polling()
    await engine.dispose()


app = FastAPI(
    title="Crypto Anomaly Detection API",
    description=(
        "REST and WebSocket API for querying real-time crypto anomaly events "
        "detected from Binance trade streams."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(anomalies.router)
app.include_router(websocket.router)
