"""
Deduplication Store - PostgreSQL Implementation
Dengan transaksi ACID, upsert, dan kontrol konkurensi.

Isolation Level: READ COMMITTED (default PostgreSQL)
- Mencegah dirty reads
- UNIQUE constraint (topic, event_id) memberikan serializability untuk dedup
- UPDATE ... SET count = count + 1 mencegah lost-update pada stats
"""

import asyncpg
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
import os

logger = logging.getLogger(__name__)


class DedupStore:
    """
    Deduplication store dengan PostgreSQL backend.
    Thread-safe via koneksi asyncpg.
    Persistent untuk durability.
    """

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize dedup store

        Args:
            database_url: PostgreSQL connection URL
        """
        self.database_url = database_url or os.getenv(
            "DATABASE_URL",
            "postgres://struser:strpass@localhost:5432/strdb"
        )
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Create connection pool"""
        try:
            self._pool = await asyncpg.create_pool(
                self.database_url,
                min_size=2,
                max_size=10,
                command_timeout=30
            )
            logger.info("Connected to PostgreSQL dedup store")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {str(e)}")
            raise

    async def close(self):
        """Close connection pool"""
        if self._pool:
            await self._pool.close()
            logger.info("Dedup store connection pool closed")

    async def check_health(self) -> bool:
        """Check database connectivity"""
        if not self._pool:
            return False
        try:
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return False

    async def exists(self, topic: str, event_id: str) -> bool:
        """
        Check apakah event sudah diproses sebelumnya

        Args:
            topic: Event topic
            event_id: Event ID

        Returns:
            bool: True jika event sudah diproses
        """
        if not self._pool:
            raise RuntimeError("Database not connected")
        try:
            async with self._pool.acquire() as conn:
                result = await conn.fetchrow(
                    "SELECT 1 FROM processed_events WHERE topic = $1 AND event_id = $2 LIMIT 1",
                    topic, event_id
                )
                return result is not None
        except Exception as e:
            logger.error(f"Error checking dedup: {str(e)}")
            raise

    async def mark_processed(
        self,
        topic: str,
        event_id: str,
        event_source: str,
        event_payload: Dict[str, Any],
        event_timestamp: datetime,
        use_outbox: bool = True
    ) -> dict:
        """
        Mark event sebagai sudah diproses — atomic dalam satu transaksi.

        Transaksi mencakup:
        1. INSERT ke processed_events dengan UNIQUE constraint
        2. INSERT ke outbox (jika use_outbox=True)
        3. UPDATE stat counter secara atomik

        Isolation: READ COMMITTED + UNIQUE constraint mencegah race condition.
        Dua worker paralel dengan (topic, event_id) yang sama:
        - Hanya SATU yang berhasil INSERT
        - Yang lain kena unique violation dan di-ignore

        Args:
            topic: Event topic
            event_id: Event ID
            event_source: Event source
            event_payload: Event payload dict
            event_timestamp: Event timestamp
            use_outbox: Whether to also write to outbox table

        Returns:
            dict: {'action': 'inserted'|'duplicate', 'outbox_id': int|None}
        """
        if not self._pool:
            raise RuntimeError("Database not connected")

        try:
            async with self._pool.acquire() as conn:
                # Begin transaction explicitly
                # Isolation level: READ COMMITTED (default PostgreSQL)
                async with conn.transaction():
                    # Step 1: INSERT dengan ON CONFLICT untuk dedup atomik
                    # READ COMMITTED + UNIQUE constraint:
                    #   - Worker A dan B sama-sama coba INSERT (topic, event_id) yang sama
                    #   - Hanya satu yang commit, satunya kena unique violation
                    #   - ON CONFLICT DO NOTHING menangani conflict gracefully
                    result = await conn.execute(
                        """
                        INSERT INTO processed_events (topic, event_id, event_source, event_payload, event_timestamp)
                        VALUES ($1, $2, $3, $4::jsonb, $5)
                        ON CONFLICT (topic, event_id) DO NOTHING
                        """,
                        topic, event_id, event_source,
                        event_payload, event_timestamp
                    )

                    # Parse INSERT result: "INSERT 0 1" = inserted, "INSERT 0 0" = conflict
                    is_inserted = "INSERT 0 1" in result

                    if is_inserted:
                        # Step 2: Update stat counter atomically
                        # Menggunakan UPDATE counter = counter + 1 mencegah lost-update
                        # (Atom: dua worker paralel hasilnya tetap akurat)
                        await conn.execute(
                            "SELECT increment_stat('unique_processed', 1)"
                        )

                        # Step 3 (Opsional): Insert ke outbox dalam transaksi yang SAMA
                        outbox_id = None
                        if use_outbox:
                            outbox_row = await conn.fetchrow(
                                """
                                INSERT INTO outbox (topic, event_id, payload)
                                VALUES ($1, $2, $3::jsonb)
                                ON CONFLICT (topic, event_id) DO NOTHING
                                RETURNING id
                                """,
                                topic, event_id,
                                event_payload
                            )
                            if outbox_row:
                                outbox_id = outbox_row['id']
                                logger.debug(f"Outbox entry created: id={outbox_id}")

                        # Step 4: Audit log
                        await conn.execute(
                            """
                            INSERT INTO audit_log (event_id, topic, action, details)
                            VALUES ($1, $2, 'processed', $3::jsonb)
                            """,
                            event_id, topic,
                            '{"source": "' + event_source + '"}'
                        )

                        logger.info(
                            f"Event processed: topic={topic}, event_id={event_id}, "
                            f"outbox_id={outbox_id}"
                        )

                        return {
                            'action': 'inserted',
                            'outbox_id': outbox_id
                        }
                    else:
                        # Step 2b: Increment duplicate counter
                        await conn.execute(
                            "SELECT increment_stat('duplicate_dropped', 1)"
                        )

                        # Audit log for duplicate
                        await conn.execute(
                            """
                            INSERT INTO audit_log (event_id, topic, action, details)
                            VALUES ($1, $2, 'duplicate_dropped', $3::jsonb)
                            """,
                            event_id, topic,
                            '{"reason": "unique constraint violation"}'
                        )

                        logger.debug(
                            f"Duplicate dropped: topic={topic}, event_id={event_id}"
                        )

                        return {
                            'action': 'duplicate',
                            'outbox_id': None
                        }

        except asyncpg.UniqueViolationError:
            # Safety net: jika ON CONFLICT tidak menangkap
            logger.warning(f"Unique violation caught (safety net): {topic}/{event_id}")
            await self._increment_stat_internal('duplicate_dropped')
            return {'action': 'duplicate', 'outbox_id': None}

        except Exception as e:
            logger.error(f"Error marking event processed: {str(e)}")
            raise

    async def increment_stat(self, stat_key: str, increment: int = 1):
        """Increment stat counter secara atomik"""
        if not self._pool:
            raise RuntimeError("Database not connected")
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "SELECT increment_stat($1, $2)",
                    stat_key, increment
                )
        except Exception as e:
            logger.error(f"Error incrementing stat {stat_key}: {str(e)}")

    async def _increment_stat_internal(self, stat_key: str):
        """Internal helper untuk increment stat"""
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "SELECT increment_stat($1, 1)",
                    stat_key
                )
        except Exception:
            pass

    async def get_stats(self) -> Dict[str, Any]:
        """Get all statistics"""
        if not self._pool:
            raise RuntimeError("Database not connected")
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT stat_key, stat_value FROM event_stats"
                )
                stats = {row['stat_key']: row['stat_value'] for row in rows}
                return {
                    'received': stats.get('received', 0),
                    'unique_processed': stats.get('unique_processed', 0),
                    'duplicate_dropped': stats.get('duplicate_dropped', 0),
                    'outbox_processed': stats.get('outbox_processed', 0),
                }
        except Exception as e:
            logger.error(f"Error getting stats: {str(e)}")
            raise

    async def get_topics(self) -> List[str]:
        """Get list of unique topics"""
        if not self._pool:
            raise RuntimeError("Database not connected")
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
                )
                return [row['topic'] for row in rows]
        except Exception as e:
            logger.error(f"Error getting topics: {str(e)}")
            raise

    async def get_processed_events(
        self, topic: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get processed events, optionally filtered by topic"""
        if not self._pool:
            raise RuntimeError("Database not connected")
        try:
            async with self._pool.acquire() as conn:
                if topic:
                    rows = await conn.fetch(
                        """
                        SELECT topic, event_id, event_source, event_payload,
                               event_timestamp, processed_at
                        FROM processed_events
                        WHERE topic = $1
                        ORDER BY processed_at DESC
                        LIMIT $2 OFFSET $3
                        """,
                        topic, limit, offset
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT topic, event_id, event_source, event_payload,
                               event_timestamp, processed_at
                        FROM processed_events
                        ORDER BY processed_at DESC
                        LIMIT $1 OFFSET $2
                        """,
                        limit, offset
                    )
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting processed events: {str(e)}")
            raise

    async def get_processed_count(self) -> int:
        """Get total unique events processed"""
        if not self._pool:
            raise RuntimeError("Database not connected")
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT stat_value FROM event_stats WHERE stat_key = 'unique_processed'"
                )
                return row['stat_value'] if row else 0
        except Exception as e:
            logger.error(f"Error getting processed count: {str(e)}")
            raise

    async def process_outbox_batch(self, batch_size: int = 10) -> int:
        """
        Process outbox entries atomically.
        Menggunakan versioning untuk mencegah double-processing.

        Returns:
            int: Number of outbox entries processed
        """
        if not self._pool:
            raise RuntimeError("Database not connected")
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # SELECT ... FOR UPDATE SKIP LOCKED untuk menghindari deadlock
                    # antar worker yang memproses outbox
                    rows = await conn.fetch(
                        """
                        UPDATE outbox
                        SET status = 'processing',
                            processed_at = NOW(),
                            version = version + 1
                        WHERE id IN (
                            SELECT id FROM outbox
                            WHERE status = 'pending'
                            ORDER BY created_at ASC
                            LIMIT $1
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING id, topic, event_id, payload
                        """,
                        batch_size
                    )

                    if not rows:
                        return 0

                    # Process each outbox entry
                    for row in rows:
                        # Simulasi proses outbox (contoh: kirim webhook, notifikasi, dsb)
                        logger.debug(
                            f"Outbox processed: id={row['id']}, "
                            f"topic={row['topic']}, event_id={row['event_id']}"
                        )

                    # Mark as completed
                    ids = [row['id'] for row in rows]
                    await conn.execute(
                        """
                        UPDATE outbox
                        SET status = 'completed', version = version + 1
                        WHERE id = ANY($1::bigint[])
                        """,
                        ids
                    )

                    # Update counter
                    await conn.execute(
                        "SELECT increment_stat('outbox_processed', $1)",
                        len(rows)
                    )

                    return len(rows)

        except Exception as e:
            logger.error(f"Error processing outbox batch: {str(e)}")
            raise

    async def get_outbox_pending_count(self) -> int:
        """Get count of pending outbox entries"""
        if not self._pool:
            raise RuntimeError("Database not connected")
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT COUNT(*) as cnt FROM outbox WHERE status = 'pending'"
                )
                return row['cnt'] if row else 0
        except Exception as e:
            logger.error(f"Error getting outbox count: {str(e)}")
            raise

    async def reset_stats(self):
        """Reset all stats (untuk testing)"""
        if not self._pool:
            raise RuntimeError("Database not connected")
        try:
            async with self._pool.acquire() as conn:
                await conn.execute("UPDATE event_stats SET stat_value = 0, updated_at = NOW()")
                logger.warning("Stats reset")
        except Exception as e:
            logger.error(f"Error resetting stats: {str(e)}")
            raise

    async def clear_all(self):
        """Clear all data (untuk testing)"""
        if not self._pool:
            raise RuntimeError("Database not connected")
        try:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM processed_events")
                await conn.execute("DELETE FROM outbox")
                await conn.execute("DELETE FROM audit_log")
                await conn.execute("UPDATE event_stats SET stat_value = 0, updated_at = NOW()")
                logger.warning("All data cleared")
        except Exception as e:
            logger.error(f"Error clearing data: {str(e)}")
            raise
