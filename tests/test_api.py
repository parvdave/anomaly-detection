"""
Tests for the FastAPI application.

Uses httpx AsyncClient with dependency overrides to mock the DB session.
Run with: pytest tests/test_api.py -v
Requires: pip install httpx pytest-asyncio
"""

import os
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pytest.importorskip("httpx", reason="httpx not installed")
pytest.importorskip("fastapi", reason="fastapi not installed")

from httpx import ASGITransport, AsyncClient

from api.src.main import app
from api.src.db import get_db
from api.src.services.alert_broadcaster import broadcaster


# ── Fixtures ───────────────────────────────────────────────────────────────────

def _make_anomaly(id=1, symbol="BTCUSDT"):
    m = MagicMock()
    m.id = id
    m.symbol = symbol
    m.event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    m.price = 67000.0
    m.volume = 0.5
    m.anomaly_type = "PRICE_ZSCORE"
    m.anomaly_score = 4.2
    m.zscore = 4.2
    m.rolling_mean = 66000.0
    m.rolling_std = 238.0
    m.is_price_anomaly = True
    m.is_iqr_anomaly = False
    m.is_volume_spike = False
    m.detected_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return m


@pytest.fixture
def mock_db():
    db = AsyncMock()
    return db


@pytest.fixture(autouse=True)
def override_db(mock_db):
    async def _get_db_override():
        yield mock_db

    app.dependency_overrides[get_db] = _get_db_override
    yield
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def stop_broadcaster():
    """Prevent the broadcaster background task from starting during tests."""
    with patch.object(broadcaster, "start_polling"), patch.object(broadcaster, "stop_polling"):
        yield


# ── Health ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_health_ok(mock_db):
    mock_db.execute = AsyncMock(return_value=MagicMock())

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "healthy"
    assert body["db_connected"] is True


@pytest.mark.asyncio
async def test_health_degraded_on_db_failure(mock_db):
    mock_db.execute = AsyncMock(side_effect=Exception("DB down"))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/health")

    assert resp.status_code == 200
    assert resp.json()["status"] == "degraded"


# ── Anomalies ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_anomalies_empty(mock_db):
    result_mock = MagicMock()
    result_mock.scalars.return_value.all.return_value = []
    mock_db.execute = AsyncMock(return_value=result_mock)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/anomalies/")

    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_anomalies_returns_data(mock_db):
    anomaly = _make_anomaly(id=1, symbol="BTCUSDT")
    result_mock = MagicMock()
    result_mock.scalars.return_value.all.return_value = [anomaly]
    mock_db.execute = AsyncMock(return_value=result_mock)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/anomalies/?symbol=BTCUSDT&limit=10")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["symbol"] == "BTCUSDT"
    assert data[0]["anomaly_type"] == "PRICE_ZSCORE"


@pytest.mark.asyncio
async def test_get_anomaly_by_id_found(mock_db):
    anomaly = _make_anomaly(id=42)
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = anomaly
    mock_db.execute = AsyncMock(return_value=result_mock)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/anomalies/42")

    assert resp.status_code == 200
    assert resp.json()["id"] == 42


@pytest.mark.asyncio
async def test_get_anomaly_by_id_not_found(mock_db):
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=result_mock)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/anomalies/9999")

    assert resp.status_code == 404
