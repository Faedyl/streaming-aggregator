import json, time
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import redis.asyncio as aioredis
from .models import Event, EventBatch, PublishResponse, StatsResponse
from .db import get_pool
from .config import REDIS_URL, STREAM_NAME

router = APIRouter()
_start_time = time.time()

@router.post("/publish", status_code=202, response_model=PublishResponse)
async def publish(body: Event | EventBatch):
    r = aioredis.from_url(REDIS_URL)
    events = body.events if isinstance(body, EventBatch) else [body]
    accepted = duplicated = 0
    errors = []
    try:
        for ev in events:
            try:
                payload = ev.model_dump()
                payload["timestamp"] = ev.timestamp.isoformat()
                await r.xadd(STREAM_NAME, {"event": json.dumps(payload)})
                accepted += 1
            except Exception as e:
                errors.append(str(e))
    finally:
        await r.aclose()
    return PublishResponse(accepted=accepted, duplicated=duplicated, errors=errors)

@router.get("/events")
async def get_events(topic: str | None = None, limit: int = 100):
    limit = min(limit, 1000)
    pool = get_pool()
    async with pool.acquire() as conn:
        if topic:
            rows = await conn.fetch(
                "SELECT * FROM processed_events WHERE topic=$1 ORDER BY received_at DESC LIMIT $2",
                topic, limit
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM processed_events ORDER BY received_at DESC LIMIT $1", limit
            )
    return [dict(r) for r in rows]

@router.get("/stats", response_model=StatsResponse)
async def get_stats():
    pool = get_pool()
    async with pool.acquire() as conn:
        stat_rows = await conn.fetch("SELECT key, value FROM stats")
        topics_count = await conn.fetchval("SELECT COUNT(DISTINCT topic) FROM processed_events")
    stats = {r["key"]: r["value"] for r in stat_rows}
    received  = stats.get("received", 0)
    unique    = stats.get("unique_processed", 0)
    dup       = stats.get("duplicate_dropped", 0)
    dup_rate  = round(dup / received, 4) if received > 0 else 0.0
    return StatsResponse(
        received=received,
        unique_processed=unique,
        duplicate_dropped=dup,
        topics=topics_count or 0,
        uptime_seconds=round(time.time() - _start_time, 2),
        duplicate_rate=dup_rate,
    )

@router.get("/healthz")
async def healthz():
    db_ok = broker_ok = "ok"
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception:
        db_ok = "error"
    try:
        r = aioredis.from_url(REDIS_URL)
        await r.ping()
        await r.aclose()
    except Exception:
        broker_ok = "error"
    status = "ok" if db_ok == "ok" and broker_ok == "ok" else "degraded"
    return {"status": status, "db": db_ok, "broker": broker_ok}
