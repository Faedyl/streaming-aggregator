"""
Tests untuk Persistence dan Crash Recovery
Menguji bahwa dedup store tetap mencegah reprocessing setelah restart
"""

import pytest
import asyncio
from datetime import datetime
import os

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"),
    reason="DATABASE_URL environment variable required"
)


@pytest.fixture(scope="module")
def event_loop():
    """Create event loop"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


class TestPersistence:
    """Test persistence and durability"""

    @pytest.mark.asyncio
    async def test_15_persistence_across_restart(self):
        """
        Test bahwa data persisted setelah koneksi database ditutup dan dibuka ulang.
        Mensimulasikan container restart.
        """
        from src.dedup_store import DedupStore
        db_url = os.getenv("DATABASE_URL",
                           "postgres://struser:strpass@localhost:5432/strdb")

        # First session
        store1 = DedupStore(database_url=db_url)
        await store1.connect()

        # Insert events
        topic = "persist-test"
        events = []
        for i in range(5):
            event_id = f"evt-persist-{i}"
            events.append(event_id)
            await store1.mark_processed(
                topic=topic, event_id=event_id,
                event_source="test", event_payload={"i": i},
                event_timestamp=datetime.utcnow(), use_outbox=False
            )

        count1 = await store1.get_processed_count()
        await store1.close()

        # Simulate restart: new connection
        store2 = DedupStore(database_url=db_url)
        await store2.connect()

        # Verify all events still exist
        for event_id in events:
            exists = await store2.exists(topic, event_id)
            assert exists, f"Event {event_id} should exist after restart"

        # Verify count
        count2 = await store2.get_processed_count()
        assert count2 >= count1, \
            f"Count should persist (was {count1}, now {count2})"

        # Verify dedup masih bekerja setelah restart
        result = await store2.mark_processed(
            topic=topic, event_id=events[0],
            event_source="test", event_payload={},
            event_timestamp=datetime.utcnow(), use_outbox=False
        )
        assert result['action'] == 'duplicate', \
            "Dedup should still work after restart"

        await store2.close()

    @pytest.mark.asyncio
    async def test_16_consumer_persistence_across_restart(self):
        """
        Test bahwa consumer tetap idempotent setelah restart.
        Event yang sudah diproses sebelum restart harus di-skip setelah restart.
        """
        from src.dedup_store import DedupStore
        db_url = os.getenv("DATABASE_URL",
                           "postgres://struser:strpass@localhost:5432/strdb")

        # First session
        store1 = DedupStore(database_url=db_url)
        await store1.connect()

        topic = "consumer-restart"
        event_id = "evt-restart-test"

        # Process event (first time)
        result1 = await store1.mark_processed(
            topic=topic, event_id=event_id,
            event_source="pre-restart", event_payload={"phase": "pre"},
            event_timestamp=datetime.utcnow(), use_outbox=False
        )
        assert result1['action'] == 'inserted'

        await store1.close()

        # Simulate restart
        store2 = DedupStore(database_url=db_url)
        await store2.connect()

        # Process same event again (post-restart)
        result2 = await store2.mark_processed(
            topic=topic, event_id=event_id,
            event_source="post-restart", event_payload={"phase": "post"},
            event_timestamp=datetime.utcnow(), use_outbox=False
        )
        assert result2['action'] == 'duplicate', \
            "Event should be duplicate after restart"

        await store2.close()

    @pytest.mark.asyncio
    async def test_17_large_batch_persistence(self):
        """
        Test persistence dengan batch besar.
        Memastikan semua event tersimpan di database.
        """
        from src.dedup_store import DedupStore
        db_url = os.getenv("DATABASE_URL",
                           "postgres://struser:strpass@localhost:5432/strdb")

        store = DedupStore(database_url=db_url)
        await store.connect()

        topic = "large-persist"
        num_events = 500

        count_before = await store.get_processed_count()

        for i in range(num_events):
            await store.mark_processed(
                topic=topic, event_id=f"evt-large-{i:05d}",
                event_source="test", event_payload={"i": i},
                event_timestamp=datetime.utcnow(), use_outbox=False
            )

        count_mid = await store.get_processed_count()
        await store.close()

        # Reopen and verify
        store2 = DedupStore(database_url=db_url)
        await store2.connect()

        count_after = await store2.get_processed_count()
        assert count_after == count_mid, \
            f"Count should be same after reopen ({count_mid} vs {count_after})"

        # Verify individual events
        for i in range(num_events):
            exists = await store2.exists(topic, f"evt-large-{i:05d}")
            assert exists, f"Event {i} should exist"

        await store2.close()

    @pytest.mark.asyncio
    async def test_18_concurrent_writes_durability(self):
        """
        Test bahwa concurrent writes aman di-persist.
        Multiple workers menulis ke database secara paralel,
        dan data tetap konsisten setelahnya.
        """
        from src.dedup_store import DedupStore
        db_url = os.getenv("DATABASE_URL",
                           "postgres://struser:strpass@localhost:5432/strdb")

        store = DedupStore(database_url=db_url)
        await store.connect()

        topic = "concurrent-persist"
        num_per_worker = 100
        num_workers = 5

        async def write_batch(worker_id: int):
            for i in range(num_per_worker):
                await store.mark_processed(
                    topic=topic,
                    event_id=f"evt-cp-{worker_id}-{i:05d}",
                    event_source=f"worker-{worker_id}",
                    event_payload={"worker": worker_id, "seq": i},
                    event_timestamp=datetime.utcnow(),
                    use_outbox=False
                )

        # Run concurrent writes
        await asyncio.gather(*[write_batch(w) for w in range(num_workers)])

        await store.close()

        # Verify persistence
        store2 = DedupStore(database_url=db_url)
        await store2.connect()

        total_expected = num_per_worker * num_workers
        for w in range(num_workers):
            for i in range(num_per_worker):
                exists = await store2.exists(
                    topic, f"evt-cp-{w}-{i:05d}"
                )
                assert exists, \
                    f"Event worker={w}, seq={i} should persist after concurrent write"

        await store2.close()
