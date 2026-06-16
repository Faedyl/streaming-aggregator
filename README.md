# UAS Sistem Terdistribusi
## Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi (Docker Compose Wajib)

### Status
✅ **In Development** — 21/21 tests passing target

### Video Demo
**YouTube:** https://youtu.be/REPLACE_WITH_VIDEO_ID
<!-- TODO: Ganti dengan link video demo YouTube (unlisted/public, durasi ≥ 25 menit) -->

---

### Struktur Folder

```
UAS/
├── docker-compose.yml           # Multi-service: aggregator + publisher + broker + storage
├── README.md                    # File ini
│
├── aggregator/                  # Aggregator service (FastAPI)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── __init__.py
│       ├── __main__.py          # Entry point
│       ├── main.py              # Uvicorn bootstrap
│       ├── app.py               # FastAPI application factory
│       ├── models.py            # Pydantic models
│       ├── dedup_store.py       # PostgreSQL dedup store (transactions, upsert, outbox)
│       ├── consumer.py          # Multi-worker idempotent consumer (Redis)
│       └── utils.py             # Metrics & logging
│
├── publisher/                   # Publisher service
│   ├── Dockerfile
│   ├── requirements.txt
│   └── publisher.py             # Event generator + duplikasi (≥30%)
│
├── broker/                      # Broker initialization
│   └── init.sql                 # PostgreSQL schema (tables, indexes, functions)
│
├── tests/                       # Tests (aggregator)
│   ├── test_dedup.py            # Dedup logic + concurrency tests
│   ├── test_api.py              # API endpoint tests
│   ├── test_persistence.py      # Crash recovery & durability tests
│   └── test_performance.py      # Stress & throughput tests
│
└── docs/
    └── report.md                # Laporan teori + implementasi
```

### Arsitektur Sistem

```
┌─────────────┐     POST /publish     ┌──────────────────┐
│  Publisher   │ ──────────────────→  │   Aggregator API  │
│  (simulator) │                      │   (FastAPI)       │
└─────────────┘                      └────────┬─────────┘
                                              │
                                              │ Enqueue unique events
                                              ▼
                                      ┌──────────────────┐
                                      │   Redis Broker    │
                                      │  (events:queue)   │
                                      └────────┬─────────┘
                                               │
                                   ┌───────────┼───────────┐
                                   ▼           ▼           ▼
                          ┌───────────────────────────────┐
                          │  Multi-Worker Consumer (x N)  │
                          │  (BRPOP → process → commit)   │
                          └───────────────┬───────────────┘
                                          │
                                          │ Transaction (READ COMMITTED):
                                          │ 1. INSERT processed_events (ON CONFLICT DO NOTHING)
                                          │ 2. INSERT outbox (same transaction)
                                          │ 3. UPDATE stats (counter + 1)
                                          ▼
                              ┌──────────────────────┐
                              │   PostgreSQL 16       │
                              │   - processed_events  │
                              │   - outbox            │
                              │   - event_stats       │
                              │   - audit_log         │
                              └──────────────────────┘
```

### Teknologi Stack

| Komponen | Teknologi |
|----------|-----------|
| Framework API | FastAPI (async) |
| Database | PostgreSQL 16-alpine (ACID, READ COMMITTED) |
| Message Broker | Redis 7-alpine |
| Consumer Workers | 4 (configurable via `NUM_WORKERS`) |
| Async Runtime | asyncio |
| Testing | pytest (21 tests) |
| Container | Docker + Docker Compose |
| Python | 3.11-slim |

### Fitur Utama

#### ✅ Idempotent Consumer & Deduplication
- Event dengan `(topic, event_id)` sama hanya diproses **sekali**
- PostgreSQL UNIQUE constraint `(topic, event_id)` sebagai mekanisme dedup
- `INSERT ... ON CONFLICT DO NOTHING` untuk dedup atomik
- **100% accurate** — tidak ada double processing

#### ✅ Transaksi & Kontrol Konkurensi (Bab 8-9)
- **Isolation Level: READ COMMITTED** (default PostgreSQL)
  - Mencegah dirty reads
  - UNIQUE constraint memberikan serializability untuk dedup
  - UPDATE `counter = counter + 1` mencegah lost-update pada stats
- **Atomic dedup**: INSERT dalam transaksi, conflict = duplicate
- **Multi-worker**: N workers paralel, Redis BRPOP untuk queue, PostgreSQL transaksi per event

#### ✅ Outbox Pattern
- Outbox entry dibuat dalam **transaksi yang SAMA** dengan dedup insert
- `SELECT ... FOR UPDATE SKIP LOCKED` untuk processing outbox tanpa deadlock
- Versioning pada outbox untuk mencegah double-processing

#### ✅ Persistensi (Volume)
- PostgreSQL: `pg_data` volume (`/var/lib/postgresql/data`)
- Redis: `broker_data` volume (`/data`)
- Aggregator: `aggregator_data` volume (`/app/data`)
- Data aman meski container dihapus/recreate

#### ✅ Fault Tolerance & Crash Recovery
- At-least-once delivery: publisher dapat retry
- Dedup store mencegah reprocessing setelah restart
- Graceful shutdown: consumer workers cancelled
- Healthcheck + readiness + liveness probes

### API Endpoints

#### POST /publish
Publish single atau batch event.

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "topic": "app-logs",
      "event_id": "evt-001",
      "timestamp": "2024-04-24T12:00:00Z",
      "source": "service-a",
      "payload": {"level": "INFO", "message": "Started"}
    }]
  }'
```

Response:
```json
{
  "status": "success",
  "received": 1,
  "processed": 1,
  "duplicates_detected": 0,
  "timestamp": "2024-04-24T12:00:01Z"
}
```

#### GET /events
Dapatkan daftar event unik yang telah diproses.

```bash
curl "http://localhost:8080/events?topic=app-logs&limit=10"
```

#### GET /stats
Statistik sistem lengkap.

```bash
curl http://localhost:8080/stats
```

Response:
```json
{
  "received": 25000,
  "unique_processed": 17500,
  "duplicate_dropped": 7500,
  "outbox_processed": 17500,
  "topics": ["app-logs", "system-events", "business-metrics", "security-audit"],
  "uptime_seconds": 3600,
  "dedup_rate": 30.0,
  "isolation_level": "READ COMMITTED (default PostgreSQL)",
  "timestamp": "2024-04-24T12:00:05Z"
}
```

#### GET /health, /readiness, /liveness
Probe untuk observability.

```bash
curl http://localhost:8080/health
curl http://localhost:8080/readiness
curl http://localhost:8080/liveness
```

#### GET /outbox/status
Status outbox processing.

### Quick Start

```bash
# Clone repository
cd UAS/

# Build dan jalankan semua service
docker compose up --build

# Akses aggregator
curl http://localhost:8080/health

# Publisher akan otomatis mengirim 25.000 event dengan 30% duplikasi
# Bisa juga kirim event manual:
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"events": [{"topic": "test", "event_id": "manual-001", "timestamp": "2024-01-01T00:00:00Z", "source": "manual", "payload": {}}]}'

# Lihat stats
curl http://localhost:8080/stats
```

### Menjalankan Tests

Tests memerlukan koneksi ke PostgreSQL yang berjalan.

```bash
# Pastikan service storage sudah jalan
docker compose up -d storage broker

# Jalankan tests
cd aggregator
DATABASE_URL=postgres://struser:strpass@localhost:5432/strdb python -m pytest tests/ -v

# Atau via Docker
docker compose run --rm aggregator python -m pytest tests/ -v
```

### Test Coverage (21 Tests)

**test_dedup.py (14 tests):**
1. `test_01_mark_and_check` — Mark + check processed
2. `test_02_duplicate_rejection` — Duplikasi rejection
3. `test_03_cross_topic_independence` — Cross-topic independence
4. `test_04_get_processed_count` — Count integrity
5. `test_05_concurrent_same_event` — Race condition prevention ⭐
6. `test_06_concurrent_different_events` — Parallel processing
7. `test_07_outbox_creation` — Outbox in transaction
8. `test_08_outbox_no_double_entry` — No duplicate outbox
9. `test_09_outbox_processing` — Outbox batch process
10. `test_10_stats_after_operations` — Stats consistency
11. `test_11_stat_atomic_increment` — Lost-update prevention ⭐
12. `test_12_get_topics` — Topics listing
13. `test_13_get_processed_events` — Events retrieval
14. `test_14_get_events_filtered_by_topic` — Topic filter

**test_api.py (10 tests):**
- `/health`, `/liveness`, `/readiness`
- `/publish` single, batch, duplicate, invalid
- `/events` filter, limit, validation
- `/stats` completeness, `/outbox/status`
- End-to-end idempotency (3x send)

**test_persistence.py (4 tests):**
- Dedup store reopen
- Consumer restart idempotency
- Large batch durability
- Concurrent writes persistence

**test_performance.py (3 tests):**
- High-volume throughput (2000+ events)
- Dedup accuracy with 70% duplication
- High-contention concurrency (8 workers × 200 events)

### Isolation Level: READ COMMITTED

Pemilihan isolation level **READ COMMITTED** (default PostgreSQL):

**Alasan:**
- **Dirty reads**: PostgreSQL mencegah dirty reads di semua isolation level
- **Non-repeatable reads**: Tolerable untuk log aggregator (tidak kritis)
- **Phantom reads**: Tidak relevan (hanya INSERT, bukan range query)
- **Lost-update**: Dicegah dengan `UPDATE counter = counter + 1` (atomik)

**Mekanisme konkurensi:**
- **Dedup**: UNIQUE constraint `(topic, event_id)` + `ON CONFLICT DO NOTHING`
  - Worker A INSERT → sukses
  - Worker B INSERT (same key) → conflict → duplicate
- **Stats**: `UPDATE event_stats SET stat_value = stat_value + 1` (atomic increment)
- **Outbox**: `SELECT ... FOR UPDATE SKIP LOCKED` mencegah deadlock

### Event Model

```json
{
  "topic": "string (alphanumeric-dash, 1-255 chars)",
  "event_id": "string (unique per topic, UUID recommended)",
  "timestamp": "ISO8601 (e.g., 2024-04-24T12:00:00Z)",
  "source": "string (publisher/source name)",
  "payload": { "key1": "value1", ... }
}
```

### Model Transaksi (Wajib)

#### 1. Dedup berbasis constraint unik (wajib)
```sql
INSERT INTO processed_events (topic, event_id, ...)
VALUES ($1, $2, ...)
ON CONFLICT (topic, event_id) DO NOTHING;
```
Dua worker paralel: hanya SATU yang berhasil INSERT.

#### 2. Outbox + upsert (opsional — diimplementasikan)
Outbox ditulis dalam transaksi yang SAMA dengan dedup insert. Proses outbox menggunakan `SELECT ... FOR UPDATE SKIP LOCKED`.

#### 3. Batch atomic (opsional)
Setiap event dalam batch diproses independen. Partial commit: event unik tetap masuk queue.

#### 4. Konsistensi statistik (opsional — diimplementasikan)
```sql
UPDATE event_stats SET stat_value = stat_value + 1 WHERE stat_key = 'unique_processed';
```
Atomik: dua worker paralel hasilnya tetap akurat.

### Asumsi & Limitasi

**Asumsi:**
1. Event ID unik per topic
2. Event source tidak digunakan untuk dedup
3. Redis queue non-persistent (events in-flight bisa hilang saat crash)
4. Partial commit untuk batch

**Limitasi:**
1. Single aggregator node (dapat di-scale horizontal)
2. Redis non-persistent (optional: Redis dengan AOF)
3. PostgreSQL single instance

**Future Improvements:**
- [ ] Redis persistent (AOF) untuk queue durability
- [ ] Prometheus metrics export
- [ ] Consumer group dengan partition
- [ ] API key authentication
- [ ] Event TTL/retention policy

### Referensi

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed systems: Concepts and design* (5th ed.). Addison-Wesley.
