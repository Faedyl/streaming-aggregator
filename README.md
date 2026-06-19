# Pub-Sub Log Aggregator

**Stack**: Python 3.11 · FastAPI · Redis Streams · PostgreSQL 16 · Docker Compose

> Proyek UAS Mata Kuliah Sistem Terdistribusi — Idempotent Consumer + Deduplication + Transaksi.
> Semua service berjalan **100% lokal** di Docker Compose — tidak ada dependensi ke layanan cloud publik.

---

## Dokumen Terkait

| Dokumen | Deskripsi |
|---------|-----------|
| [report.md](report.md) | Laporan UAS (T1–T10, analisis performa, daftar pustaka APA 7th) |

---

## Arsitektur Sistem

```mermaid
flowchart LR
    PUB["publisher\n(simulator)"]

    subgraph COMPOSE["Docker Compose Network — internal only"]
        direction TB

        subgraph AGG["aggregator :8080"]
            direction TB
            API["FastAPI Routes\n/publish  /events  /stats  /healthz"]
            CW["Consumer Worker x3\nasyncio background tasks"]
        end

        REDIS[("Redis 7-alpine\nStreams")]
        PG[("PostgreSQL 16\nprocessed_events\nstats · audit_log")]
    end

    DISK[("pg_data volume\npersistent disk")]
    K6["k6\nload test"]
    PUB -->|"POST /publish\nbatch JSON"| API
    API -->|"XADD events"| REDIS
    REDIS -->|"XREADGROUP\nGROUP agg"| CW
    CW -->|"INSERT ON CONFLICT DO NOTHING\nUPDATE stats"| PG
    PG --- DISK
    K6 -->|"POST /publish\n20k events · 35% dup"| API
```

### Poin Desain

| Komponen | Keputusan | Alasan |
|---|---|---|
| Broker | Redis Streams | At-least-once native via ACK mechanism |
| Dedup store | PostgreSQL `UNIQUE(topic, event_id)` | Atomic, tahan restart, ACID |
| Multi-worker | Redis consumer group (3 consumer) | Distribusi otomatis, tidak double-process |
| Isolation | `READ COMMITTED` | Cukup dengan UNIQUE constraint, overhead rendah |
| Persistensi | Named volumes `pg_data`, `broker_data` | Survive `docker compose down` tanpa `-v` |

---

## Alur Publish-Consume

```mermaid
sequenceDiagram
    autonumber
    participant PUB as publisher
    participant AGG as aggregator
    participant RED as Redis Streams
    participant CW  as consumer worker
    participant PG  as PostgreSQL

    PUB  ->> AGG : POST /publish { event JSON }
    AGG  ->> RED : XADD events { event_json }
    AGG -->> PUB : 202 Accepted { accepted: N }

    loop Consumer Group Loop — BLOCK 1000ms
        CW  ->> RED : XREADGROUP GROUP agg consumer-N COUNT 10 BLOCK 1000
        RED -->> CW : msg_id, event_json

        CW  ->> PG  : BEGIN TRANSACTION isolation=read_committed
        CW  ->> PG  : INSERT processed_events ON CONFLICT (topic, event_id) DO NOTHING RETURNING id

        alt Event baru (unique)
            PG  -->> CW : RETURNING id  (row inserted)
            CW  ->>  PG : UPDATE stats SET unique_processed = unique_processed + 1
            CW  ->>  PG : INSERT audit_log action='inserted'
        else Event duplikat
            PG  -->> CW : no row returned
            CW  ->>  PG : UPDATE stats SET duplicate_dropped = duplicate_dropped + 1
            CW  ->>  PG : INSERT audit_log action='duplicate'
        end

        CW  ->> PG  : COMMIT
        CW  ->> RED : XACK events agg msg_id
    end
```

---

## Topologi Jaringan

```mermaid
flowchart TB
    HOST["Host Machine\nlocalhost"]

    subgraph BRIDGE["compose_default — bridge network (internal)"]
        direction TB
        AGG["aggregator\n:8080"]
        REDIS["broker  Redis\n:6379  INTERNAL"]
        PG["storage  PostgreSQL\n:5432  INTERNAL"]
        PUB["publisher\nprofile: load"]
        K6["k6\nprofile: load"]
    end

    HOST -->|"8080:8080  EXPOSED"| AGG
    AGG  <-->|"redis://broker:6379"| REDIS
    AGG  <-->|"postgres://storage:5432"| PG
    PUB  -->|"http://aggregator:8080/publish"| AGG
    K6   -->|"http://aggregator:8080/publish"| AGG

    NOTE1["broker port 6379 — NOT exposed to host"]
    NOTE2["storage port 5432 — NOT exposed to host"]
```

---

## Skema Database

```mermaid
erDiagram
    PROCESSED_EVENTS {
        bigserial id           PK
        text      topic        "NOT NULL"
        text      event_id     "NOT NULL"
        text      source       "NOT NULL"
        jsonb     payload      "NOT NULL"
        timestamptz event_timestamp "NOT NULL"
        timestamptz received_at     "DEFAULT NOW()"
    }

    STATS {
        text   key   PK
        bigint value     "DEFAULT 0"
    }

    AUDIT_LOG {
        bigserial   id         PK
        timestamptz event_time "DEFAULT NOW()"
        text        action     "inserted | duplicate | error"
        text        topic
        text        event_id
        jsonb       detail
    }
```

**Constraint kunci:** `UNIQUE (topic, event_id)` pada tabel `processed_events` — fondasi dari seluruh mekanisme deduplication.

---

## Cara Menjalankan

```bash
# 1. Clone repo
git clone https://github.com/[username]/uts-distrib.git
cd uts-distrib

# 2. Jalankan core services
docker compose up -d --build

# 3. Cek health
curl http://localhost:8080/healthz
# {"status":"ok","db":"ok","broker":"ok"}

# 4. Publish event baru
curl -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"demo","event_id":"E001","timestamp":"2025-01-15T10:00:00Z","source":"cli","payload":{"v":1}}'

# 5. Kirim duplikat (event_id sama)
curl -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"demo","event_id":"E001","timestamp":"2025-01-15T10:00:01Z","source":"cli","payload":{"v":1}}'

# 6. Cek stats (duplicate_dropped harus = 1)
curl http://localhost:8080/stats | python3 -m json.tool

# 7. Lihat events
curl "http://localhost:8080/events?topic=demo&limit=10"

# 8. Load test (20k event, 35% duplikat)
docker compose --profile load run --rm k6

# 9. Publisher simulator (20k event @ 40% duplikat)
docker compose --profile load up publisher
```

## Menjalankan Tests

```bash
# Di host (butuh Postgres + Redis running)
cd aggregator
pip install -r requirements.txt
pytest tests/ -v

# Di dalam container
docker compose run --rm aggregator pytest tests/ -v
```

---

## Endpoints

| Method | Path | Deskripsi | Response |
|--------|------|-----------|----------|
| `POST` | `/publish` | Kirim single atau batch event | `202 {"accepted":N,"duplicated":M}` |
| `GET` | `/events?topic=X&limit=100` | Daftar event unik yang diproses | `200 [{...}]` |
| `GET` | `/stats` | Statistik: received, unique, dup, uptime | `200 {"received":N,...}` |
| `GET` | `/healthz` | Health check DB + broker | `200 {"status":"ok"}` |

### Contoh Response `/stats`

```json
{
  "received":          20000,
  "unique_processed":  13021,
  "duplicate_dropped":  6979,
  "topics":               20,
  "uptime_seconds":     87.4,
  "duplicate_rate":    0.3490
}
```

---

## Bukti Persistensi

```bash
# Catat stats sebelum stop
curl http://localhost:8080/stats

# Stop container — TANPA -v (volume AMAN)
docker compose stop

# Start ulang
docker compose start
sleep 10

# Cek stats sesudah — angka HARUS sama
curl http://localhost:8080/stats
```

Named volumes `pg_data` dan `broker_data` hanya dihapus jika eksplisit `docker compose down -v`. Container recreate biasa tidak menghapus data.

---

## Distribusi Tests

Total **19 tests** dalam 6 file:

| # | File | Cakupan |
|---|------|---------|
| 1–3 | `test_dedup.py` | Insert baru, duplikat return False, hanya 1 row di DB |
| 4–7 | `test_api.py` | POST single, POST batch, GET /events, GET /stats fields |
| 8–10 | `test_concurrency.py` | 50 parallel insert sama, stats no lost-update, no double-process |
| 11–12 | `test_persistence.py` | Data survive reconnect, duplikat tetap ditolak setelah reconnect |
| 13–15 | `test_validation.py` | Missing field 422, invalid timestamp 422, empty batch 400/422 |
| 16–19 | `test_termshot.py` | scripts/ ada, chmod +x, screenshots/ dibuat, PNG magic bytes valid |

---

## Video Demo

> Minimal 25 menit, YouTube unlisted atau public.

Cantumkan di sini setelah upload:

```
Video Demo: https://youtube.com/watch?v=LINK_ANDA
```

Poin yang harus ditampilkan di video:
- Arsitektur multi-service dan alasan desain
- `docker compose up --build` dari nol
- Kirim event duplikat + bukti idempotency di `/stats`
- Demonstrasi transaksi/konkurensi (multi-worker) — output test
- `GET /events` dan `GET /stats` sebelum/sesudah load
- Crash/recreate container + bukti data persisten via volumes
- Keamanan jaringan lokal (broker/storage tidak terekspos ke host)
- Observability: logging, metrik

---

## Asumsi & Catatan

- Broker (Redis 6379) dan Storage (Postgres 5432) **tidak** di-expose ke host.
- Named volumes `pg_data` dan `broker_data` survive `docker compose down` (tanpa `-v`).
- Consumer group Redis: 3 consumer paralel, at-least-once + idempotent dedup.
- Isolation level: `READ COMMITTED` + UNIQUE constraint.

## Referensi

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012).
*Distributed systems: Concepts and design* (5th ed.). Addison-Wesley.
