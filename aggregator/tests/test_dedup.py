"""
Tests untuk Deduplication Logic dengan PostgreSQL
Menguji idempotency, UNIQUE constraint, isolation level
"""

import pytest
import asyncio
from datetime import datetime
import os
import json

# Skip jika DATABASE_URL tidak di-set (untuk local dev)
pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"),
    reason="DATABASE_URL environment variable required"
)


@pytest.fixture(scope="module")
def event_loop():
    """Create event loop for module-scoped fixtures"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def dedup_store():
    """Create dedup store connected to test database"""
    from src.dedup_store import DedupStore
    # Use DATABASE_URL for test (parallel test database or same)
    db_url = os.getenv("DATABASE_URL",
                       "postgres://struser:strpass@localhost:5432/strdb")
    store = DedupStore(database_url=db_url)
    await store.connect()
    await store.clear_all()
    yield store
    await store.close()


class TestDedupBasic:
    """Test basic deduplication functionality"""

    @pytest.mark.asyncio
    async def test_01_mark_and_check(self, dedup_store):
        """Test marking event and checking existence"""
        topic = "test-topic"
        event_id = f"evt-{uuid.uuid4().hex[:8]}"

        # Should not exist initially
        exists = await dedup_store.exists(topic, event_id)
        assert not exists, "Event should not exist before processing"

        # Mark as processed
        result = await dedup_store.mark_processed(
            topic=topic,
            event_id=event_id,
            event_source="test-source",
            event_payload={"test": "data"},
            event_timestamp=datetime.utcnow(),
            use_outbox=False
        )
        assert result['action'] == 'inserted', "Should be inserted"

        # Should exist now
        exists = await dedup_store.exists(topic, event_id)
        assert exists, "Event should exist after processing"

    @pytest.mark.asyncio
    async def test_02_duplicate_rejection(self, dedup_store):
        """Test that duplicate events are rejected"""
        topic = "dup-test"
        event_id = f"evt-dup-{uuid.uuid4().hex[:8]}"

        # First insert - should succeed
        result1 = await dedup_store.mark_processed(
            topic=topic,
            event_id=event_id,
            event_source="source-a",
            event_payload={"seq": 1},
            event_timestamp=datetime.utcnow(),
            use_outbox=False
        )
        assert result1['action'] == 'inserted'

        # Second insert with same (topic, event_id) - should be flagged as duplicate
        result2 = await dedup_store.mark_processed(
            topic=topic,
            event_id=event_id,
            event_source="source-b",
            event_payload={"seq": 2},
            event_timestamp=datetime.utcnow(),
            use_outbox=False
        )
        assert result2['action'] == 'duplicate', \
            "Same (topic, event_id) should be detected as duplicate"

    @pytest.mark.asyncio
    async def test_03_cross_topic_independence(self, dedup_store):
        """Test same event_id on different topics are treated separately"""
        event_id = f"evt-cross-{uuid.uuid4().hex[:8]}"

        # Insert on topic A
        result_a = await dedup_store.mark_processed(
            topic="topic-a", event_id=event_id,
            event_source="src", event_payload={},
            event_timestamp=datetime.utcnow(), use_outbox=False
        )
        assert result_a['action'] == 'inserted'

        # Same event_id on topic B should be independent
        exists_b = await dedup_store.exists("topic-b", event_id)
        assert not exists_b, "Same event_id on different topic should not exist"

        # Insert on topic B
        result_b = await dedup_store.mark_processed(
            topic="topic-b", event_id=event_id,
            event_source="src", event_payload={},
            event_timestamp=datetime.utcnow(), use_outbox=False
        )
        assert result_b['action'] == 'inserted', \
            "Same event_id on different topic should be allowed"

    @pytest.mark.asyncio
    async def test_04_get_processed_count(self, dedup_store):
        """Test getting count of processed events"""
        # Get current count
        count_before = await dedup_store.get_processed_count()
        assert isinstance(count_before, int)

        # Add a few unique events
        for i in range(5):
            await dedup_store.mark_processed(
                topic="count-test",
                event_id=f"evt-cnt-{uuid.uuid4().hex[:8]}",
                event_source="src",
                event_payload={"i": i},
                event_timestamp=datetime.utcnow(),
                use_outbox=False
            )

        count_after = await dedup_store.get_processed_count()
        assert count_after == count_before + 5, \
            f"Count should increase by 5 (was {count_before}, now {count_after})"


class TestDedupConcurrency:
    """Test concurrent event processing - race condition prevention"""

    @pytest.mark.asyncio
    async def test_05_concurrent_same_event(self, dedup_store):
        """
        Test konkurensi: dua worker mencoba memproses event yang SAMA.

        Ekspektasi:
        - Hanya SATU worker yang berhasil (action='inserted')
        - Worker lain gagal (action='duplicate')
        - Tidak ada double processing
        - Count hanya increment sekali
        """
        topic = "concurrent-test"
        event_id = f"evt-concurrent-{uuid.uuid4().hex[:8]}"

        async def try_process(worker_id: int) -> dict:
            """Simulasi worker mencoba memproses event yang sama"""
            return await dedup_store.mark_processed(
                topic=topic,
                event_id=event_id,
                event_source=f"worker-{worker_id}",
                event_payload={"worker_id": worker_id},
                event_timestamp=datetime.utcnow(),
                use_outbox=False
            )

        # Jalankan 5 worker secara paralel yang mencoba insert event SAMA
        results = await asyncio.gather(*[try_process(i) for i in range(5)])

        # Hitung berapa yang berhasil
        inserted = sum(1 for r in results if r['action'] == 'inserted')
        duplicates = sum(1 for r in results if r['action'] == 'duplicate')

        print(f"\nConcurrent test: {inserted} inserted, {duplicates} duplicates")

        # Harusnya hanya 1 inserted, sisanya duplicate
        assert inserted == 1, \
            f"Only 1 worker should succeed, got {inserted}"
        assert duplicates == 4, \
            f"4 workers should fail, got {duplicates}"

        # Verify only 1 count increase
        exists = await dedup_store.exists(topic, event_id)
        assert exists, "Event should exist in dedup store"

    @pytest.mark.asyncio
    async def test_06_concurrent_different_events(self, dedup_store):
        """
        Test konkurensi: multiple workers memproses event BERBEDA secara paralel.

        Ekspektasi:
        - Semua worker berhasil (tidak ada konflik)
        - Count sesuai jumlah event
        """
        num_workers = 10

        async def process_unique(worker_id: int) -> dict:
            """Worker memproses event unik"""
            return await dedup_store.mark_processed(
                topic="parallel-test",
                event_id=f"evt-par-{uuid.uuid4().hex[:8]}",
                event_source=f"worker-{worker_id}",
                event_payload={"worker_id": worker_id},
                event_timestamp=datetime.utcnow(),
                use_outbox=False
            )

        results = await asyncio.gather(*[process_unique(i) for i in range(num_workers)])

        inserted = sum(1 for r in results if r['action'] == 'inserted')
        assert inserted == num_workers, \
            f"All {num_workers} workers should succeed, got {inserted}"


class TestOutboxPattern:
    """Test outbox pattern implementation"""

    @pytest.mark.asyncio
    async def test_07_outbox_creation(self, dedup_store):
        """Test that outbox entries are created in same transaction"""
        topic = "outbox-test"
        event_id = f"evt-out-{uuid.uuid4().hex[:8]}"

        result = await dedup_store.mark_processed(
            topic=topic, event_id=event_id,
            event_source="test", event_payload={"key": "value"},
            event_timestamp=datetime.utcnow(),
            use_outbox=True  # Enable outbox
        )

        # outbox_id should be set when use_outbox=True
        assert result['action'] == 'inserted'
        assert result['outbox_id'] is not None, \
            "Outbox entry should be created with ID"

    @pytest.mark.asyncio
    async def test_08_outbox_no_double_entry(self, dedup_store):
        """
        Test bahwa outbox tidak membuat entry ganda.

        Jika event duplikat, outbox juga tidak boleh dibuat ulang
        karena dalam transaksi yang SAMA dengan dedup.
        """
        topic = "outbox-dup-test"
        event_id = f"evt-outdup-{uuid.uuid4().hex[:8]}"

        # First: create event + outbox
        result1 = await dedup_store.mark_processed(
            topic=topic, event_id=event_id,
            event_source="src", event_payload={},
            event_timestamp=datetime.utcnow(),
            use_outbox=True
        )
        assert result1['action'] == 'inserted'
        outbox_id_1 = result1['outbox_id']

        # Second: duplicate - should NOT create new outbox entry
        result2 = await dedup_store.mark_processed(
            topic=topic, event_id=event_id,
            event_source="src", event_payload={},
            event_timestamp=datetime.utcnow(),
            use_outbox=True
        )
        assert result2['action'] == 'duplicate'
        assert result2['outbox_id'] is None, \
            "Duplicate should not create outbox entry"

    @pytest.mark.asyncio
    async def test_09_outbox_processing(self, dedup_store):
        """Test outbox batch processing"""
        # Create some outbox entries
        for i in range(5):
            await dedup_store.mark_processed(
                topic="outbox-proc",
                event_id=f"evt-op-{uuid.uuid4().hex[:8]}",
                event_source="src", event_payload={"i": i},
                event_timestamp=datetime.utcnow(),
                use_outbox=True
            )

        # Process outbox
        processed = await dedup_store.process_outbox_batch(batch_size=10)
        assert processed == 5, f"Should process 5 outbox entries, got {processed}"

        # No more pending
        pending = await dedup_store.get_outbox_pending_count()
        assert pending == 0, "No outbox entries should remain pending"


class TestStatsConsistency:
    """Test transactional stat consistency"""

    @pytest.mark.asyncio
    async def test_10_stats_after_operations(self, dedup_store):
        """Test that stats are consistent after operations"""
        stats = await dedup_store.get_stats()

        assert 'received' in stats
        assert 'unique_processed' in stats
        assert 'duplicate_dropped' in stats
        assert all(isinstance(v, int) for v in stats.values()), \
            "All stats values should be integers"

    @pytest.mark.asyncio
    async def test_11_stat_atomic_increment(self, dedup_store):
        """
        Test atomic stat increment menggunakan
        UPDATE counter = counter + 1 (lost-update prevention)
        """
        # Get initial value
        before = (await dedup_store.get_stats())['unique_processed']

        # Increment concurrently
        num_increments = 20
        await asyncio.gather(*[
            dedup_store.increment_stat('unique_processed')
            for _ in range(num_increments)
        ])

        after = (await dedup_store.get_stats())['unique_processed']
        assert after == before + num_increments, \
            f"Count should increase by {num_increments} " \
            f"(before={before}, after={after})"


class TestTopics:
    """Test topics functionality"""

    @pytest.mark.asyncio
    async def test_12_get_topics(self, dedup_store):
        """Test getting list of unique topics"""
        topics = await dedup_store.get_topics()
        assert isinstance(topics, list)
        # Should contain topics from previous tests
        assert "test-topic" in topics or "dup-test" in topics or \
               "concurrent-test" in topics or "parallel-test" in topics


class TestProcessedEvents:
    """Test retrieving processed events"""

    @pytest.mark.asyncio
    async def test_13_get_processed_events(self, dedup_store):
        """Test getting processed events list"""
        events = await dedup_store.get_processed_events(limit=10)
        assert isinstance(events, list)
        assert len(events) <= 10

        if events:
            ev = events[0]
            assert 'topic' in ev
            assert 'event_id' in ev
            assert 'event_payload' in ev

    @pytest.mark.asyncio
    async def test_14_get_events_filtered_by_topic(self, dedup_store):
        """Test getting events filtered by topic"""
        # Create a test topic
        test_topic = f"filter-test-{uuid.uuid4().hex[:4]}"
        for i in range(3):
            await dedup_store.mark_processed(
                topic=test_topic,
                event_id=f"evt-flt-{i}",
                event_source="src",
                event_payload={"i": i},
                event_timestamp=datetime.utcnow(),
                use_outbox=False
            )

        events = await dedup_store.get_processed_events(topic=test_topic)
        assert len(events) == 3, \
            f"Should get 3 events for topic {test_topic}, got {len(events)}"
        for ev in events:
            assert ev['topic'] == test_topic
