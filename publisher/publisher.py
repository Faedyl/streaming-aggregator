"""
Publisher untuk Docker Compose demo
Mengirim event dengan duplikasi tinggi (≥30%) untuk testing idempotency + transaksi
"""

import asyncio
import httpx
import logging
import os
import random
from datetime import datetime
from uuid import uuid4
from typing import List

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def publish_batch(aggregator_url: str, events: List[dict], batch_num: int):
    """
    Publish batch events ke aggregator

    Args:
        aggregator_url: URL aggregator
        events: List of events
        batch_num: Batch sequence number (for logging)
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{aggregator_url}/publish",
                json={"events": events},
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                logger.info(
                    f"[Batch {batch_num}] Sent {data['received']} events, "
                    f"processed: {data['processed']}, "
                    f"duplicates: {data['duplicates_detected']}"
                )
            else:
                logger.error(
                    f"[Batch {batch_num}] Error: {response.status_code} - {response.text}"
                )

    except httpx.RequestError as e:
        logger.error(f"[Batch {batch_num}] Request failed: {str(e)}")
        # Retry logic
        await asyncio.sleep(2)
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{aggregator_url}/publish",
                    json={"events": events},
                    timeout=30
                )
                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"[Batch {batch_num}] Retry success: {data}")
        except Exception as retry_err:
            logger.error(f"[Batch {batch_num}] Retry also failed: {str(retry_err)}")


def generate_events(count: int, topics: List[str], source: str) -> List[dict]:
    """
    Generate test events

    Args:
        count: Number of events
        topics: List of topics to distribute
        source: Source name

    Returns:
        List of event dicts
    """
    events = []
    for i in range(count):
        topic = topics[i % len(topics)]
        event = {
            "topic": topic,
            "event_id": f"evt-{i:06d}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": source,
            "payload": {
                "sequence": i,
                "level": random.choice(["INFO", "WARN", "ERROR"]),
                "message": f"Test event {i} for topic {topic}",
                "user_id": uuid4().hex[:8],
                "duration_ms": random.randint(10, 5000)
            }
        }
        events.append(event)
    return events


def add_duplicates(events: List[dict], duplicate_rate: float,
                   original_events: List[dict]) -> List[dict]:
    """
    Tambahkan duplikasi ke batch events

    Strategi duplikasi:
    - Ambil beberapa event dari original_events (event terdahulu)
    - Tambahkan sebagai duplikat di batch saat ini
    - Duplikat memiliki event_id yang SAMA dengan event asli

    Args:
        events: Current batch events
        duplicate_rate: Fraction of events to duplicate (0.0 - 1.0)
        original_events: Pool of previously generated events

    Returns:
        Events with duplicates added
    """
    if not original_events:
        return events

    result = list(events)
    num_duplicates = max(1, int(len(events) * duplicate_rate))

    for _ in range(num_duplicates):
        if original_events:
            original = random.choice(original_events)
            # Same event_id = duplicate, randomize other fields
            duplicate = {
                "topic": original["topic"],
                "event_id": original["event_id"],  # Same ID = duplicate
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source": original["source"],
                "payload": {
                    **original["payload"],
                    "duplicate_flag": True,
                    "resent_at": datetime.utcnow().isoformat()
                }
            }
            result.append(duplicate)

    return result


async def main():
    """Main publisher function"""
    aggregator_url = os.getenv("AGGREGATOR_URL", "http://aggregator:8080")
    num_events = int(os.getenv("NUM_EVENTS", "25000"))
    duplication_rate = float(os.getenv("DUPLICATION_RATE", "0.30"))
    batch_size = int(os.getenv("BATCH_SIZE", "100"))

    topics = ["app-logs", "system-events", "business-metrics", "security-audit"]
    source = f"publisher-{os.getenv('HOSTNAME', 'local')}"

    logger.info("=" * 60)
    logger.info("UAS Publisher Starting...")
    logger.info(f"Aggregator: {aggregator_url}")
    logger.info(f"Total events to generate: {num_events}")
    logger.info(f"Duplication rate: {duplication_rate * 100:.0f}%")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Topics: {topics}")
    logger.info("=" * 60)

    # Generate all events
    logger.info("Generating events...")
    all_events = generate_events(num_events, topics, source)
    logger.info(f"Generated {len(all_events)} base events")

    # Tunggu aggregator siap
    await asyncio.sleep(10)

    # Kirim events dalam batch dengan duplikasi
    logger.info("Publishing events with duplicates...")
    total_sent = 0
    batch_num = 0
    original_pool = []

    for start_idx in range(0, len(all_events), batch_size):
        batch_num += 1
        batch = all_events[start_idx:start_idx + batch_size]

        # Add duplicates (ambil dari original_pool = events yang sudah dikirim)
        batch_with_dupes = add_duplicates(batch, duplication_rate, original_pool)

        # Add to original pool (tanpa duplikat)
        original_pool.extend(batch)

        # Kirim batch
        await publish_batch(aggregator_url, batch_with_dupes, batch_num)
        total_sent += len(batch_with_dupes)

        # Progress log
        if batch_num % 10 == 0:
            logger.info(
                f"Progress: {total_sent} events sent "
                f"({(start_idx + batch_size) / num_events * 100:.0f}%)"
            )

        # Small delay to avoid flooding
        await asyncio.sleep(0.5)

    logger.info("=" * 60)
    logger.info(f"Publisher finished: {total_sent} total events sent")
    logger.info(f"Base events: {num_events}, "
                f"Expected duplicates: ~{int(num_events * duplication_rate)}")

    # Wait for processing
    logger.info("Waiting for queue to drain...")
    await asyncio.sleep(10)

    # Print final stats
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{aggregator_url}/stats")
            if response.status_code == 200:
                stats = response.json()
                logger.info("=" * 60)
                logger.info("FINAL STATISTICS:")
                logger.info(f"  Received:           {stats['received']}")
                logger.info(f"  Unique Processed:   {stats['unique_processed']}")
                logger.info(f"  Duplicate Dropped:  {stats['duplicate_dropped']}")
                logger.info(f"  Outbox Processed:   {stats['outbox_processed']}")
                logger.info(f"  Topics:             {stats['topics']}")
                logger.info(f"  Dedup Rate:         {stats['dedup_rate']}%")
                logger.info(f"  Uptime:             {stats['uptime_seconds']}s")
                logger.info(f"  Isolation Level:    {stats['isolation_level']}")
                logger.info("=" * 60)

            # Show duplicate proof
            resp2 = await client.get(f"{aggregator_url}/events?topic=app-logs&limit=3")
            if resp2.status_code == 200:
                events_data = resp2.json()
                logger.info(f"Sample processed events: {events_data['count']} available")
    except Exception as e:
        logger.error(f"Error getting final stats: {str(e)}")

    logger.info("Publisher demo complete!")


if __name__ == "__main__":
    asyncio.run(main())
