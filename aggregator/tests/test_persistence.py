import pytest, uuid
from app.dedup import process_event
from app.db import init_db, close_db, get_pool

@pytest.mark.asyncio
async def test_data_survives_reconnect():
    """Tutup pool, buka ulang, data tetap ada."""
    from datetime import datetime, timezone
    ev = {"topic":"persist","event_id":str(uuid.uuid4()),
          "source":"t","payload":{},"timestamp":datetime.now(timezone.utc).isoformat()}
    await process_event(ev)
    await close_db()
    await init_db()
    pool = get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM processed_events WHERE event_id=$1", ev["event_id"])
    assert count == 1

@pytest.mark.asyncio
async def test_duplicate_blocked_after_reconnect():
    """Setelah reconnect, event yang sama masih ditolak."""
    from datetime import datetime, timezone
    ev = {"topic":"persist2","event_id":str(uuid.uuid4()),
          "source":"t","payload":{},"timestamp":datetime.now(timezone.utc).isoformat()}
    first = await process_event(ev)
    await close_db()
    await init_db()
    second = await process_event(ev)
    assert first is True
    assert second is False
