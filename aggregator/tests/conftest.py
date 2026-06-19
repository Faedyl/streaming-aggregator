import asyncio, os, pytest, asyncpg
import redis.asyncio as aioredis
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.db import init_db, close_db, get_pool
from app.config import DATABASE_URL, REDIS_URL, STREAM_NAME, GROUP_NAME

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session", autouse=True)
async def setup_db():
    await init_db()
    yield
    # cleanup
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE processed_events, audit_log RESTART IDENTITY")
        await conn.execute("UPDATE stats SET value=0")
    await close_db()

@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

@pytest.fixture
async def pool():
    return get_pool()
