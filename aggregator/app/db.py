import asyncpg, pathlib
from .config import DATABASE_URL

_pool: asyncpg.Pool | None = None

async def init_db():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
    schema = pathlib.Path(__file__).parent / "schema.sql"
    async with _pool.acquire() as conn:
        await conn.execute(schema.read_text())

async def close_db():
    if _pool:
        await _pool.close()

def get_pool() -> asyncpg.Pool:
    return _pool
