import pytest, asyncio, uuid
from app.dedup import process_event

@pytest.mark.asyncio
async def test_50_parallel_inserts_same_event(pool):
    """50 insert paralel event sama → hanya 1 row baru."""
    from datetime import datetime, timezone
    ev = {
        "topic": "concurrent", "event_id": str(uuid.uuid4()),
        "source": "test", "payload": {}, "timestamp": datetime.now(timezone.utc).isoformat()
    }
    results = await asyncio.gather(*[process_event(dict(ev)) for _ in range(50)])
    assert sum(results) == 1, f"Diharapkan 1 inserted, dapat {sum(results)}"

@pytest.mark.asyncio
async def test_concurrent_stats_no_lost_update(pool):
    """100 stat increment paralel → nilai akhir konsisten."""
    async with pool.acquire() as conn:
        before = await conn.fetchval("SELECT value FROM stats WHERE key='received'")
    from datetime import datetime, timezone
    events = [
        {"topic":"stat-test","event_id":str(uuid.uuid4()),
         "source":"t","payload":{},"timestamp":datetime.now(timezone.utc).isoformat()}
        for _ in range(100)
    ]
    await asyncio.gather(*[process_event(e) for e in events])
    async with pool.acquire() as conn:
        after = await conn.fetchval("SELECT value FROM stats WHERE key='received'")
    assert after - before == 100

@pytest.mark.asyncio
async def test_no_double_process_multi_event(pool):
    """Verifikasi tidak ada duplikat di tabel meski dipanggil paralel."""
    from datetime import datetime, timezone
    eid = str(uuid.uuid4())
    ev  = {"topic":"nodup","event_id":eid,"source":"t","payload":{},"timestamp":datetime.now(timezone.utc).isoformat()}
    await asyncio.gather(*[process_event(dict(ev)) for _ in range(20)])
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM processed_events WHERE topic='nodup' AND event_id=$1", eid)
    assert count == 1
