"""
AlertBroadcaster

Manages WebSocket connections and fans out real-time anomaly alerts.

Detection loop: polls anomaly_events every POLL_INTERVAL seconds using a
last_seen_id watermark to avoid re-broadcasting.  For sub-second latency
consider switching to PostgreSQL LISTEN/NOTIFY.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.websockets import WebSocket, WebSocketDisconnect

from ..models.db import AnomalyEvent
from ..models.schemas import AnomalyEventResponse

logger = logging.getLogger(__name__)

POLL_INTERVAL = 2  # seconds between DB polls


class AlertBroadcaster:
    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._last_seen_id: int = 0
        self._poll_task: asyncio.Task | None = None

    # ── Connection management ──────────────────────────────────────────────────

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)
        logger.info("WS client connected. Total: %d", len(self._connections))

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(websocket)
        logger.info("WS client disconnected. Total: %d", len(self._connections))

    # ── Broadcast ──────────────────────────────────────────────────────────────

    async def broadcast(self, payload: dict) -> None:
        async with self._lock:
            connections = set(self._connections)

        if not connections:
            return

        dead: set[WebSocket] = set()
        for ws in connections:
            try:
                await ws.send_json(payload)
            except (WebSocketDisconnect, RuntimeError):
                dead.add(ws)

        if dead:
            async with self._lock:
                self._connections -= dead

    # ── Background polling task ────────────────────────────────────────────────

    def start_polling(self, session_factory: async_sessionmaker) -> None:
        self._poll_task = asyncio.create_task(
            self._poll_loop(session_factory), name="anomaly-poll"
        )

    def stop_polling(self) -> None:
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()

    async def _poll_loop(self, session_factory: async_sessionmaker) -> None:
        # Initialise watermark to the current max id so we only broadcast
        # anomalies that arrive after the API starts.
        async with session_factory() as db:
            from sqlalchemy import func

            result = await db.execute(
                select(func.max(AnomalyEvent.id))
            )
            max_id = result.scalar_one_or_none()
            self._last_seen_id = max_id or 0

        logger.info("Alert broadcaster started. Watermark id=%d", self._last_seen_id)

        while True:
            try:
                await asyncio.sleep(POLL_INTERVAL)
                await self._fetch_and_broadcast(session_factory)
            except asyncio.CancelledError:
                logger.info("Alert broadcaster stopped.")
                return
            except Exception as exc:
                logger.error("Poll error: %s", exc, exc_info=True)

    async def _fetch_and_broadcast(self, session_factory: async_sessionmaker) -> None:
        async with session_factory() as db:
            result = await db.execute(
                select(AnomalyEvent)
                .where(AnomalyEvent.id > self._last_seen_id)
                .order_by(AnomalyEvent.id.asc())
                .limit(100)
            )
            new_events = list(result.scalars().all())

        for event in new_events:
            payload = AnomalyEventResponse.model_validate(event).model_dump(mode="json")
            await self.broadcast(payload)
            self._last_seen_id = max(self._last_seen_id, event.id)

        if new_events:
            logger.info("Broadcasted %d new anomaly events.", len(new_events))


# Singleton instance shared across the app lifecycle
broadcaster = AlertBroadcaster()


def get_broadcaster() -> AlertBroadcaster:
    return broadcaster
