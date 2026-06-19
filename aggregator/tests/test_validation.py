import pytest

@pytest.mark.asyncio
async def test_missing_required_field(client):
    r = await client.post("/publish", json={
        "event_id": "x", "timestamp": "2025-01-01T00:00:00Z",
        "source": "t", "payload": {}
        # topic HILANG
    })
    assert r.status_code == 422

@pytest.mark.asyncio
async def test_invalid_timestamp(client):
    r = await client.post("/publish", json={
        "topic": "t", "event_id": "e1",
        "timestamp": "bukan-timestamp",
        "source": "t", "payload": {}
    })
    assert r.status_code == 422

@pytest.mark.asyncio
async def test_empty_batch_rejected(client):
    r = await client.post("/publish", json={"events": []})
    assert r.status_code in (400, 422)
