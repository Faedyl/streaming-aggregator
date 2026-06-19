import json, logging
from datetime import datetime
from .db import get_pool

logger = logging.getLogger(__name__)

def _parse_ts(ts_str: str):
    """Parse ISO timestamp string to datetime. Returns None on failure."""
    if not ts_str:
        return None
    try:
        ts = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ts)
    except Exception:
        logger.warning("Cannot parse timestamp: %s", ts_str)
        return None

async def process_event(event) -> bool:
    """
    Insert event ke DB dalam satu transaksi:
      - INSERT ... ON CONFLICT DO NOTHING
      - UPDATE stats (unique_processed atau duplicate_dropped)
      - INSERT audit_log
    Return True = baru diproses, False = duplikat.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="read_committed"):
            row = await conn.fetchrow(
                """
                INSERT INTO processed_events(topic, event_id, source, payload, event_timestamp)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (topic, event_id) DO NOTHING
                RETURNING id
                """,
                event["topic"],
                event["event_id"],
                event["source"],
                json.dumps(event["payload"]),
                _parse_ts(event.get("timestamp", event.get("event_timestamp", ""))),
            )
            inserted = row is not None

            stat_key = "unique_processed" if inserted else "duplicate_dropped"
            await conn.execute(
                "UPDATE stats SET value = value + 1 WHERE key = $1", stat_key
            )
            await conn.execute(
                "UPDATE stats SET value = value + 1 WHERE key = 'received'"
            )

            action = "inserted" if inserted else "duplicate"
            await conn.execute(
                """
                INSERT INTO audit_log(action, topic, event_id, detail)
                VALUES ($1, $2, $3, $4)
                """,
                action,
                event["topic"],
                event["event_id"],
                "{}",
            )

            if inserted:
                logger.info("INSERTED topic=%s event_id=%s", event["topic"], event["event_id"])
            else:
                logger.info("DUPLICATE DROPPED topic=%s event_id=%s", event["topic"], event["event_id"])

            return inserted
