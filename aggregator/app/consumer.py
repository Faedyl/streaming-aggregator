import asyncio, json, logging, os
import redis.asyncio as aioredis
from .config import REDIS_URL, STREAM_NAME, GROUP_NAME, CONSUMER_WORKERS
from .dedup import process_event

logger = logging.getLogger(__name__)

class ConsumerWorker:
    def __init__(self):
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def _ensure_group(self, r):
        """Create stream and consumer group if they don't exist."""
        try:
            await r.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
            logger.info("Consumer group '%s' created/verified", GROUP_NAME)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info("Consumer group '%s' already exists", GROUP_NAME)
            else:
                logger.warning("xgroup_create error: %s", e)

    async def _worker(self, worker_id: int):
        """Single consumer worker — XREADGROUP loop."""
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        consumer_name = f"worker-{worker_id}"
        logger.info("Worker %s started", consumer_name)

        while self._running:
            try:
                results = await r.xreadgroup(
                    GROUP_NAME, consumer_name,
                    {STREAM_NAME: ">"},
                    count=10,
                    block=2000,
                )
                if not results:
                    continue

                for stream_name, messages in results:
                    for msg_id, msg_data in messages:
                        try:
                            event_raw = msg_data.get("event", "{}")
                            event = json.loads(event_raw) if isinstance(event_raw, str) else event_raw
                            # Ensure payload is a dict
                            if isinstance(event.get("payload"), str):
                                event["payload"] = json.loads(event["payload"])
                            elif not isinstance(event.get("payload"), dict):
                                event["payload"] = {}
                            await process_event(event)
                            await r.xack(STREAM_NAME, GROUP_NAME, msg_id)
                        except Exception as e:
                            logger.error("Worker %s error processing %s: %s", worker_id, msg_id, e)
            except Exception as e:
                logger.error("Worker %s XREADGROUP error: %s", worker_id, e)
                await asyncio.sleep(1)

        await r.aclose()
        logger.info("Worker %s stopped", consumer_name)

    async def start(self):
        self._running = True
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        await self._ensure_group(r)
        await r.aclose()

        for i in range(CONSUMER_WORKERS):
            task = asyncio.create_task(self._worker(i))
            self._tasks.append(task)
        logger.info("%d consumer workers started", CONSUMER_WORKERS)

    async def stop(self):
        self._running = False
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()
        logger.info("All consumer workers stopped")
