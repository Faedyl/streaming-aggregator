import pytest, uuid
from datetime import datetime, timezone

def evt(**kw):
    base = {
        "topic":     "api-test",
        "event_id":  str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source":    "pytest",
        "payload":   {"x": 1},
    }
    base.update(kw)
    return base

@pytest.mark.asyncio
async def test_publish_single_event(client):
    r = await client.post("/publish", json=evt())
    assert r.status_code == 202
    data = r.json()
    assert "accepted" in data
    assert "duplicated" in data

@pytest.mark.asyncio
async def test_publish_batch(client):
    batch = {"events": [evt() for _ in range(10)]}
    r = await client.post("/publish", json=batch)
    assert r.status_code == 202
    assert r.json()["accepted"] == 10

@pytest.mark.asyncio
async def test_get_events_returns_list(client):
    await client.post("/publish", json=evt(topic="events-test"))
    import asyncio; await asyncio.sleep(1)  # consumer memproses
    r = await client.get("/events", params={"topic": "events-test"})
    assert r.status_code == 200
    assert isinstance(r.json(), list)

@pytest.mark.asyncio
async def test_get_stats_has_required_fields(client):
    r = await client.get("/stats")
    assert r.status_code == 200
    data = r.json()
    for field in ("received","unique_processed","duplicate_dropped","topics","uptime_seconds","duplicate_rate"):
        assert field in data, f"missing: {field}"
