"""
Consumer Logic - Idempotent Event Processing dengan Multi-Worker

Mendukung multiple concurrent workers yang memproses event dari Redis queue.
Setiap worker memproses event dalam transaksi PostgreSQL sendiri.
Tidak ada dua worker yang bisa memproses event (topic, event_id) yang sama
karena UNIQUE constraint + ON CONFLICT DO NOTHING.
"""

import asyncio
import json
import logging
import os
from typing import Optional, List
from datetime import datetime
from .models import Event
from .dedup_store import DedupStore
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class IdempotentConsumer:
    """
    Idempotent consumer multi-worker yang memproses event hanya sekali
    meskipun event duplikat diterima berkali-kali.

    Concurrency Model:
    - Multiple asyncio workers menarik event dari Redis queue (BRPOP)
    - Setiap worker memproses event dalam transaksi PostgreSQL sendiri
    - UNIQUE constraint (topic, event_id) di PostgreSQL mencegah double-processing
    - Worker yang kalah (conflict) akan log sebagai duplicate dan skip
    """

    def __init__(self, dedup_store: DedupStore, broker_url: Optional[str] = None):
        """
        Initialize consumer

        Args:
            dedup_store: DedupStore instance
            broker_url: Redis broker URL
        """
        self.dedup_store = dedup_store
        self.broker_url = broker_url or os.getenv(
            "BROKER_URL", "redis://localhost:6379"
        )
        self._redis: Optional[aioredis.Redis] = None
        self.running = False
        self.workers: List[asyncio.Task] = []
        self.num_workers = int(os.getenv("NUM_WORKERS", "4"))
        self.queue_key = "events:queue"
        self.processed_events: dict = {}  # In-memory LRU cache

    async def start(self):
        """Start consumer workers"""
        logger.info(f"Starting consumer with {self.num_workers} workers...")

        # Connect ke Redis
        try:
            self._redis = aioredis.Redis.from_url(
                self.broker_url,
                decode_responses=True
            )
            await self._redis.ping()
            logger.info("Connected to Redis broker")
        except Exception as e:
            logger.error(f"Error connecting to Redis: {str(e)}")
            raise

        self.running = True

        # Start multiple workers
        for i in range(self.num_workers):
            worker = asyncio.create_task(
                self._worker(i),
                name=f"consumer-worker-{i}"
            )
            self.workers.append(worker)
            logger.info(f"Consumer worker {i} started")

    async def stop(self):
        """Stop consumer gracefully"""
        logger.info("Stopping consumer...")
        self.running = False

        # Cancel all workers
        for worker in self.workers:
            worker.cancel()

        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)

        # Close Redis connection
        if self._redis:
            await self._redis.close()
            logger.info("Redis connection closed")

        self.workers = []
        logger.info("Consumer stopped")

    async def _worker(self, worker_id: int):
        """
        Background worker - process events from Redis queue

        Setiap worker:
        1. BRPOP dari Redis queue (blocking pop)
        2. Parse event JSON
        3. Process via dedup store (transaksi PostgreSQL)
        4. Log hasil

        Worker yang kalah (duplicate) akan:
        - Mendapat 'action': 'duplicate' dari dedup_store.mark_processed()
        - Tidak memproses ulang event
        - Hanya increment duplicate counter
        """
        while self.running:
            try:
                # BRPOP dengan timeout 1 detik
                # - Jika ada event: return (key, value)
                # - Jika timeout: return None
                result = await self._redis.brpop(
                    self.queue_key,
                    timeout=1
                )

                if result is None:
                    # Timeout, loop lagi
                    continue

                # result = (queue_key, event_json)
                event_data = json.loads(result[1])

                # Convert to Event model
                event = Event(**event_data)

                # Process melalui dedup store (transaksi PostgreSQL)
                process_result = await self.dedup_store.mark_processed(
                    topic=event.topic,
                    event_id=event.event_id,
                    event_source=event.source,
                    event_payload=event.payload,
                    event_timestamp=event.timestamp,
                    use_outbox=True
                )

                # Track in-memory (untuk fast GET /events)
                if process_result['action'] == 'inserted':
                    self.processed_events[(event.topic, event.event_id)] = {
                        "event": event.dict(),
                        "processed_at": datetime.utcnow(),
                        "status": "processed"
                    }

                logger.debug(
                    f"[Worker {worker_id}] Event {event.topic}/{event.event_id}: "
                    f"{process_result['action']}"
                )

            except asyncio.CancelledError:
                logger.info(f"Worker {worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Error in worker {worker_id}: {str(e)}")
                await asyncio.sleep(1)

    async def enqueue_event(self, event: Event):
        """
        Enqueue event ke Redis broker untuk diproses worker

        Args:
            event: Event object
        """
        if not self._redis:
            raise RuntimeError("Redis not connected")

        event_json = json.dumps(event.dict(), default=str)
        await self._redis.lpush(self.queue_key, event_json)

    async def enqueue_batch(self, events: List[Event]):
        """Enqueue multiple events ke Redis broker"""
        if not self._redis:
            raise RuntimeError("Redis not connected")

        pipe = self._redis.pipeline()
        for event in events:
            event_json = json.dumps(event.dict(), default=str)
            pipe.lpush(self.queue_key, event_json)
        await pipe.execute()

    async def get_queue_length(self) -> int:
        """Get current queue length"""
        if not self._redis:
            return 0
        try:
            return await self._redis.llen(self.queue_key)
        except Exception:
            return 0
