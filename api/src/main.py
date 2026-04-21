"""
FastAPI application factory.

Lifespan:
  - Startup: initialise DB engine, start AlertBroadcaster polling task.
  - Shutdown: stop polling, dispose DB engine.
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)

_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/", include_in_schema=False)
async def serve_dashboard():
    return FileResponse(os.path.join(_static_dir, "index.html"))


app.include_router(health.router)
app.include_router(anomalies.router)
app.include_router(websocket.router)
