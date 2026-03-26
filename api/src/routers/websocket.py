import asyncio
import logging

from fastapi import APIRouter, WebSocket
from starlette.websockets import WebSocketDisconnect

from ..services.alert_broadcaster import get_broadcaster

router = APIRouter(tags=["websocket"])
logger = logging.getLogger(__name__)


@router.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket) -> None:
    """
    Real-time anomaly alert stream.

    Connect and receive JSON anomaly events as they are detected.
    Clients may send any message to keep the connection alive (ping).
    The connection stays open until the client disconnects.
    """
    broadcaster = get_broadcaster()
    await broadcaster.connect(websocket)
    try:
        while True:
            # Wait for a client message (acts as a keep-alive receiver).
            # Timeout after 60s; if no client ping, the connection is still
            # kept alive — we just loop back.
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
            except asyncio.TimeoutError:
                pass
    except WebSocketDisconnect:
        pass
    finally:
        await broadcaster.disconnect(websocket)
