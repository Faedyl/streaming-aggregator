import pytest, uuid
from app.dedup import process_event

def make_event(**kwargs):
    from datetime import datetime, timezone
    base = {
        "topic":     "test-topic",
        "event_id":  str(uuid.uuid4()),
        "source":    "test",
        "payload":   {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    base.update(kwargs)
    return base

@pytest.mark.asyncio
async def test_insert_new_event_returns_true():
    ev = make_event()
    result = await process_event(ev)
    assert result is True

@pytest.mark.asyncio
async def test_insert_duplicate_returns_false():
    ev = make_event()
    first  = await process_event(ev)
    second = await process_event(ev)
    assert first is True
    assert second is False

@pytest.mark.asyncio
async def test_duplicate_does_not_add_to_processed_events(pool):
    ev = make_event()
    await process_event(ev)
    await process_event(ev)
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM processed_events WHERE topic=$1 AND event_id=$2",
            ev["topic"], ev["event_id"]
        )
    assert count == 1
