# Pub-Sub Log Aggregator (UAS Sistem Terdistribusi)

**Stack**: Python 3.11 · FastAPI · Redis Streams · PostgreSQL 16 · Docker Compose

## Dokumen Terkait

| Dokumen | Deskripsi |
|---------|-----------|
| [report.md](report.md) | Laporan UAS (T1–T10, analisis, daftar pustaka APA 7th) |
| [docs/report.md](docs/report.md) | Salinan laporan di folder docs |

## Arsitektur

publisher → POST /publish → aggregator (FastAPI) → Redis Streams
                                                         ↓ (consumer group)
                                                     PostgreSQL (dedup store)

## Cara Menjalankan

```bash
# 1. Clone repo
git clone https://github.com/[username]/uts-distrib.git
cd uts-distrib

# 2. Jalankan core services
docker compose up -d --build

# 3. Cek health
curl http://localhost:8080/healthz

# 4. Publish event manual (single)
curl -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"demo","event_id":"e1","timestamp":"2025-01-15T10:00:00Z","source":"cli","payload":{"v":1}}'

# 5. Publish duplikat (event_id sama)
curl -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"demo","event_id":"e1","timestamp":"2025-01-15T10:00:01Z","source":"cli","payload":{"v":1}}'

# 6. Lihat stats
curl http://localhost:8080/stats

# 7. Lihat events
curl "http://localhost:8080/events?topic=demo"

# 8. Load test dengan k6
docker compose --profile load run --rm k6

# 9. Publisher simulator (20k event @ 40% duplikat)
docker compose --profile load up publisher
```

## Menjalankan Tests

```bash
# Opsi 1: di host (perlu Postgres + Redis jalan)
cd aggregator
pip install -r requirements.txt
pytest tests/ -v

# Opsi 2: di dalam container
docker compose run --rm aggregator pytest tests/ -v
```

## Bukti Persistensi

```bash
# Stop container (TANPA -v = volume AMAN)
docker compose stop
docker compose start
curl http://localhost:8080/stats   # angka sama
```

## Endpoints

| Method | Path                      | Deskripsi                    |
|--------|---------------------------|------------------------------|
| POST   | /publish                  | Kirim single/batch event     |
| GET    | /events?topic=X&limit=100 | Daftar event unik            |
| GET    | /stats                    | Statistik agregat            |
| GET    | /healthz                  | Health check DB + broker     |

## Video Demo

🎥 https://youtube.com/watch?v=[LINK_ANDA]

## Laporan

- [📄 report.md](report.md) — Laporan UAS T1–T10 + analisis performa + daftar pustaka APA 7th

## Asumsi & Catatan

- Broker (Redis 6379) dan Storage (Postgres 5432) TIDAK di-expose ke host.
- Named volumes `pg_data` dan `broker_data` survive `docker compose down` (tanpa `-v`).
- Consumer group Redis: 3 consumer paralel, at-least-once + idempotent dedup.
- Isolation level: READ COMMITTED + UNIQUE constraint.
