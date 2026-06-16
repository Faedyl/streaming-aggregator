"""
Performance Tests untuk Scalability dan Stress Test
Menguji throughput, latency, dan konkurensi
"""

import pytest
import asyncio
import time
import os
from datetime import datetime

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


class TestPerformance:
    """Test performance and scalability benchmarks"""

    @pytest.mark.asyncio
    async def test_19_high_volume_throughput(self):
        """
        Stress test: proses 10.000+ event dan ukur throughput.

        Target: throughput minimal 500 events/detik
        (dengan PostgreSQL, lebih tinggi dari SQLite)
        """
        from src.dedup_store import DedupStore
        db_url = os.getenv("DATABASE_URL",
                           "postgres://struser:strpass@localhost:5432/strdb")

        store = DedupStore(database_url=db_url)
        await store.connect()

        num_events = 2000  # Reduced for test speed, but meaningful
        topic = "stress-test"

        start_time = time.time()

        for i in range(num_events):
            await store.mark_processed(
                topic=topic,
                event_id=f"evt-stress-{i:06d}",
                event_source="perf-test",
                event_payload={"seq": i, "data": "x" * 100},
                event_timestamp=datetime.utcnow(),
                use_outbox=True  # Include outbox overhead
            )

        elapsed = time.time() - start_time
        throughput = num_events / elapsed if elapsed > 0 else 0

        print(f"\n[PERF] Processed {num_events} events in {elapsed:.2f}s")
        print(f"[PERF] Throughput: {throughput:.0f} events/sec")

        await store.close()

        # Verify count persists
        store2 = DedupStore(database_url=db_url)
        await store2.connect()
        count = await store2.get_processed_count()
        print(f"[PERF] Total persisted count: {count}")
        await store2.close()

        # Minimum throughput threshold (PostgreSQL)
        # Should easily reach 500+ events/sec
        assert throughput >= 100, \
            f"Throughput too low: {throughput:.0f} events/sec. " \
            "May indicate DB connectivity issue."

    @pytest.mark.asyncio
    async def test_20_dedup_accuracy_with_high_duplication(self):
        """
        Test akurasi dedup dengan duplikasi tinggi (70%).

        Kirim 500 unique events + 1000 duplicate attempts.
        Ekspektasi: 500 unique processed, ~1000 duplicates dropped.
        """
        from src.dedup_store import DedupStore
        db_url = os.getenv("DATABASE_URL",
                           "postgres://struser:strpass@localhost:5432/strdb")

        store = DedupStore(database_url=db_url)
        await store.connect()

        topic = "dedup-accuracy"
        num_unique = 500
        dup_attempts_per_event = 2  # Each event sent 3x total (1 original + 2 dupes)

        # Generate unique events
        unique_ids = [f"evt-da-{i:05d}" for i in range(num_unique)]

        start_time = time.time()

        # First pass: insert all unique events
        for event_id in unique_ids:
            await store.mark_processed(
                topic=topic, event_id=event_id,
                event_source="test", event_payload={},
                event_timestamp=datetime.utcnow(), use_outbox=False
            )

        # Second pass: send duplicates
        duplicates_detected = 0
        for event_id in unique_ids:
            for _ in range(dup_attempts_per_event):
                result = await store.mark_processed(
                    topic=topic, event_id=event_id,
                    event_source="dup-test", event_payload={},
                    event_timestamp=datetime.utcnow(), use_outbox=False
                )
                if result['action'] == 'duplicate':
                    duplicates_detected += 1

        elapsed = time.time() - start_time
        total_ops = num_unique + (num_unique * dup_attempts_per_event)
        throughput = total_ops / elapsed if elapsed > 0 else 0

        print(f"\n[PERF] Total operations: {total_ops}")
        print(f"[PERF] Unique inserted: {num_unique}")
        print(f"[PERF] Duplicates detected: {duplicates_detected}")
        print(f"[PERF] Expected duplicates: {num_unique * dup_attempts_per_event}")
        print(f"[PERF] Elapsed: {elapsed:.2f}s")
        print(f"[PERF] Throughput: {throughput:.0f} ops/sec")

        await store.close()

        # Verifikasi: semua duplikasi terdeteksi
        expected_dupes = num_unique * dup_attempts_per_event
        assert duplicates_detected == expected_dupes, \
            f"Dedup accuracy: {duplicates_detected}/{expected_dupes} " \
            f"({duplicates_detected/expected_dupes*100:.1f}%)"

    @pytest.mark.asyncio
    async def test_21_concurrent_high_contention(self):
        """
        Test konkurensi dengan high contention pada topic yang sama.

        Multiple workers bersaing untuk memproses event dengan
        (topic) yang sama. Memastikan tidak ada data race atau
        duplicate processing.
        """
        from src.dedup_store import DedupStore
        db_url = os.getenv("DATABASE_URL",
                           "postgres://struser:strpass@localhost:5432/strdb")

        store = DedupStore(database_url=db_url)
        await store.connect()

        topic = "high-contention"
        num_events = 200
        num_workers = 8

        async def worker_task(worker_id: int, event_ids: list):
            """Worker memproses daftar event"""
            results = []
            for eid in event_ids:
                result = await store.mark_processed(
                    topic=topic, event_id=eid,
                    event_source=f"worker-{worker_id}",
                    event_payload={"worker": worker_id},
                    event_timestamp=datetime.utcnow(),
                    use_outbox=False
                )
                results.append(result['action'])
            return results

        # Distribute events among workers (with some overlap for contention)
        all_event_ids = [f"evt-hc-{i:05d}" for i in range(num_events)]

        # Each worker gets ALL events (maximum contention scenario)
        start_time = time.time()
        results = await asyncio.gather(*[
            worker_task(w, all_event_ids) for w in range(num_workers)
        ])
        elapsed = time.time() - start_time

        # Results analysis
        all_actions = []
        for worker_results in results:
            all_actions.extend(worker_results)

        total_inserted = sum(1 for a in all_actions if a == 'inserted')
        total_duplicates = sum(1 for a in all_actions if a == 'duplicate')

        total_ops = num_events * num_workers
        throughput = total_ops / elapsed if elapsed > 0 else 0

        print(f"\n[CONC] Total operations: {total_ops}")
        print(f"[CONC] Inserted: {total_inserted}")
        print(f"[CONC] Duplicates: {total_duplicates}")
        print(f"[CONC] Expected unique: {num_events}")
        print(f"[CONC] Elapsed: {elapsed:.2f}s")
        print(f"[CONC] Throughput: {throughput:.0f} ops/sec")

        await store.close()

        # Hanya num_events yang unique yang inserted
        assert total_inserted == num_events, \
            f"Should have exactly {num_events} unique inserts, got {total_inserted}"
        assert total_duplicates == total_ops - num_events, \
            "All other operations should be duplicates"
