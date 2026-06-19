"""Helper: atomic stat increment."""
from .db import get_pool
import logging

logger = logging.getLogger(__name__)

async def increment_stat(key: str, amount: int = 1) -> None:
    """Increment a stat counter atomically."""
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE stats SET value = value + $1 WHERE key = $2",
                amount, key
            )
    except Exception as e:
        logger.error("Failed to increment stat %s: %s", key, e)
