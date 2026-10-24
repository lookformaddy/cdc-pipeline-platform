"""Tests: FastAPI service surface (auto-skip without FastAPI)."""

import pytest

from cdc_pipeline.service import FASTAPI_AVAILABLE, create_app
from tests.conftest import make_event

pytestmark = pytest.mark.skipif(
    not FASTAPI_AVAILABLE, reason="FastAPI not installed"
)


@pytest.fixture
def client(pipeline):
    from fastapi.testclient import TestClient

    return TestClient(create_app(pipeline))


def test_health_and_ingest_and_read(client):
    assert client.get("/health").json()["status"] == "ok"
    event = make_event(1)
    response = client.post("/ingest", json={"events": [event.to_dict()]})
    assert response.status_code == 200
    assert response.json()["applied"] == 1
    row = client.get("/tables/orders/rows/ord-1")
    assert row.status_code == 200
    assert row.json()["status"] == "placed"


def test_ingest_duplicate_is_skipped(client):
    event = make_event(1)
    client.post("/ingest", json={"events": [event.to_dict()]})
    response = client.post("/ingest", json={"events": [event.to_dict()]})
    assert response.json()["duplicates_skipped"] == 1


def test_time_travel_endpoint(client):
    client.post("/ingest", json={"events": [make_event(1).to_dict()]})
    client.post("/ingest", json={"events": [make_event(2, key="ord-2").to_dict()]})
    body = client.get("/tables/orders/as-of/1").json()
    assert set(body["rows"]) == {"ord-1"}


def test_metrics_and_quarantine_endpoints(client):
    assert client.get("/metrics").status_code == 200
    body = client.get("/quarantine").json()
    assert body["count"] == 0
    assert client.get("/tables/orders/rows/missing").status_code == 404
    assert client.get("/tables/nope/rows/ord-1").status_code == 404
