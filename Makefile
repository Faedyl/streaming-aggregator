.PHONY: help install build up down logs test test-dedup test-api test-perf test-persistence clean \
	demo-ps demo-health demo-send-event demo-send-dup demo-stats demo-events \
	demo-run-pub demo-stop-agg demo-start-agg demo-logs-agg demo-verify \
	demo-dedup demo-concurrency demo-before-after demo-persist demo-cleanall \
	demo-volumes

help:
	@echo "UAS Pub-Sub Log Aggregator - Available Commands"
	@echo ""
	@echo "Docker:"
	@echo "  make build       - Build all Docker images"
	@echo "  make up          - Start all services (docker compose up)"
	@echo "  make down        - Stop all services"
	@echo "  make logs        - View logs"
	@echo ""
	@echo "Testing:"
	@echo "  make test        - Run all tests"
	@echo "  make test-dedup  - Run dedup tests"
	@echo "  make test-api    - Run API tests"
	@echo "  make test-perf   - Run performance tests"
	@echo "  make test-persist - Run persistence tests"
	@echo ""
	@echo "Setup:"
	@echo "  make install     - Install Python dependencies"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean       - Clean temporary files"
	@echo ""
	@echo "Demo (SCRIPT_VIDEO.md one-liners):"
	@echo "  make demo-ps            - docker compose ps"
	@echo "  make demo-health         - curl /healthz"
	@echo "  make demo-send-event     - Kirim 1 event DEMO-001 (Segmen 4)"
	@echo "  make demo-send-dup       - Kirim event DEMO-001 3x duplikat (Segmen 4)"
	@echo "  make demo-stats          - curl /stats"
	@echo "  make demo-events         - curl /events?limit=5"
	@echo "  make demo-run-pub        - Jalankan publisher (profile load) — Segmen 6"
	@echo "  make demo-stop-agg       - docker compose stop aggregator (Segmen 7)"
	@echo "  make demo-start-agg      - docker compose start aggregator (Segmen 7)"
	@echo "  make demo-logs-agg       - docker compose logs aggregator --tail 20"
	@echo "  make demo-volumes        - docker volume ls (pg_data / broker_data)"
	@echo "  make demo-verify         - Jalankan verify.sh (Segmen 10)"
	@echo "  make demo-dedup          - 📦 Demo dedup lengkap (Segmen 4)"
	@echo "  make demo-concurrency    - ⚡ Run test concurrency (Segmen 5)"
	@echo "  make demo-before-after   - 📊 Demo /events + /stats sebelum/sesudah (Segmen 6)"
	@echo "  make demo-persist        - 💾 Demo persistensi stop/start (Segmen 7)"

build:
	@echo "Building Docker images..."
	DOCKER_BUILDKIT=0 docker compose build

up:
	@echo "Starting services..."
	DOCKER_BUILDKIT=0 docker compose up --build -d
	@echo "Aggregator at: http://localhost:8080"
	@echo "Waiting for healthcheck..."
	@sleep 10
	@curl -s http://localhost:8080/healthz | python -m json.tool

down:
	@echo "Stopping services..."
	docker compose down

logs:
	docker compose logs -f

install:
	@echo "Creating virtual environment and installing dependencies (for linting/IDE support)..."
	cd aggregator && python3 -m venv .venv 2>/dev/null && .venv/bin/pip install --upgrade pip -q && .venv/bin/pip install -r requirements.txt -q
	@echo "✅ Dependencies installed in aggregator/.venv/"

test:
	@echo "Running all tests (via Docker)..."
	docker compose exec aggregator python -m pytest tests/ -v --tb=short

test-dedup:
	docker compose exec aggregator python -m pytest tests/test_dedup.py -v --tb=short

test-api:
	docker compose exec aggregator python -m pytest tests/test_api.py -v --tb=short

test-perf:
	docker compose exec aggregator python -m pytest tests/test_performance.py -v --tb=short -s

test-persist:
	docker compose exec aggregator python -m pytest tests/test_persistence.py -v --tb=short

clean:
	@echo "Cleaning..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov *.egg-info

# ═══════════════════════════════════════════════════════════════
# Demo — satu perintah per aksi di SCRIPT_VIDEO.md
# ═══════════════════════════════════════════════════════════════

# ── Segmen 3 ──

demo-ps:
	@docker compose ps

demo-health:
	@echo ""
	curl -s http://localhost:8080/healthz | python -m json.tool

# ── Segmen 4 — Idempotency & Dedup ──

demo-send-event:
	@echo "📨 Mengirim event DEMO-001..."
	curl -s -X POST http://localhost:8080/publish \
	  -H 'Content-Type: application/json' \
	  -d '{"topic": "order.created", "event_id": "DEMO-001", "timestamp": "2025-06-19T10:00:00Z", "source": "demo", "payload": {"item": "buku", "qty": 2}}' \
	  | python -m json.tool

demo-send-dup:
	@echo "🔄 Mengirim duplikat DEMO-001 sebanyak 3x..."
	for i in 1 2 3; do \
	  curl -s -X POST http://localhost:8080/publish \
	    -H 'Content-Type: application/json' \
	    -d '{"topic": "order.created", "event_id": "DEMO-001", "timestamp": "2025-06-19T10:00:00Z", "source": "demo", "payload": {"item": "buku", "qty": 2}}' \
	    | python -m json.tool; \
	  sleep 1; \
	done

demo-stats:
	@curl -s http://localhost:8080/stats | python -m json.tool

demo-events:
	@echo "📋 Menampilkan event terbaru (limit 5)..."
	curl -s "http://localhost:8080/events?limit=5" | python -m json.tool

demo-dedup:
	@echo "═══════════════════════════════════════"
	@echo " 📦  DEMO DEDUP — SCRIPT VIDEO Segmen 4"
	@echo "═══════════════════════════════════════"
	@echo ""
	@echo "[1/4] Mengirim event DEMO-001 (pertama kali)..."
	curl -s -X POST http://localhost:8080/publish \
	  -H 'Content-Type: application/json' \
	  -d '{"topic": "order.created", "event_id": "DEMO-001", "timestamp": "2025-06-19T10:00:00Z", "source": "demo", "payload": {"item": "buku", "qty": 2}}' \
	  | python -m json.tool
	@echo ""
	@echo "[2/4] Mengirim duplikat DEMO-001 3x..."
	for i in 1 2 3; do \
	  curl -s -X POST http://localhost:8080/publish \
	    -H 'Content-Type: application/json' \
	    -d '{"topic": "order.created", "event_id": "DEMO-001", "timestamp": "2025-06-19T10:00:00Z", "source": "demo", "payload": {"item": "buku", "qty": 2}}' \
	    | python -m json.tool; \
	  sleep 1; \
	done
	@echo ""
	@echo "[3/4] Cek stats — bukti dedup..."
	curl -s http://localhost:8080/stats | python -m json.tool
	@echo ""
	@echo "[4/4] Jalankan test dedup..."
	docker compose exec aggregator python -m pytest tests/test_dedup.py -v --tb=short
	@echo ""
	@echo "✅  DEMO DEDUP SELESAI"

# ── Segmen 5 — Concurrency ──

demo-concurrency:
	@echo "═══════════════════════════════════════"
	@echo " ⚡  DEMO KONKURENSI — SCRIPT VIDEO Segmen 5"
	@echo "═══════════════════════════════════════"
	@echo ""
	docker compose exec aggregator python -m pytest tests/test_concurrency.py -v --tb=short -s
	@echo ""
	@echo "✅  DEMO KONKURENSI SELESAI"

# ── Segmen 6 — Events & Stats Sebelum/Sesudah ──

demo-run-pub:
	@echo "🏭 Menjalankan publisher (20.000 event, 40% duplikat)..."
	docker compose --profile load run publisher

demo-before-after:
	@echo "═══════════════════════════════════════"
	@echo " 📊  DEMO EVENTS/STATS — SCRIPT VIDEO Segmen 6"
	@echo "═══════════════════════════════════════"
	@echo ""
	@echo "=== SEBELUM LOAD TEST ==="
	curl -s "http://localhost:8080/events?limit=5" | python -m json.tool
	curl -s http://localhost:8080/stats | python -m json.tool
	@echo ""
	@echo "=== Menjalankan publisher... ==="
	docker compose --profile load run publisher
	@echo ""
	@echo "=== SESUDAH LOAD TEST ==="
	curl -s "http://localhost:8080/events?limit=3" | python -m json.tool
	curl -s http://localhost:8080/stats | python -m json.tool
	@echo ""
	@echo "✅  DEMO EVENTS/STATS SELESAI"

# ── Segmen 7 — Persistensi ──

demo-stop-agg:
	@echo "🛑 Menghentikan aggregator..."
	docker compose stop aggregator
	@echo ""
	docker compose ps

demo-start-agg:
	@echo "▶️  Menyalakan aggregator..."
	docker compose start aggregator
	@echo "Menunggu healthcheck (15 detik)..."
	@sleep 15
	@echo ""
	curl -s http://localhost:8080/healthz | python -m json.tool

demo-persist:
	@echo "═══════════════════════════════════════"
	@echo " 💾  DEMO PERSISTENSI — SCRIPT VIDEO Segmen 7"
	@echo "═══════════════════════════════════════"
	@echo ""
	@echo "[1/5] Catat stats SEBELUM crash..."
	curl -s http://localhost:8080/stats | python -m json.tool
	@echo ""
	@echo "[2/5] Stop aggregator..."
	docker compose stop aggregator
	@sleep 2
	docker compose ps
	@echo ""
	@echo "[3/5] Verifikasi endpoint mati..."
	curl -sf http://localhost:8080/stats || echo "✅ GAGAL — aggregator mati (sesuai harapan)"
	@echo ""
	@echo "[4/5] Start aggregator kembali..."
	docker compose start aggregator
	@echo "Menunggu healthcheck (15 detik)..."
	@sleep 15
	curl -s http://localhost:8080/healthz | python -m json.tool
	@echo ""
	@echo "[5/5] Cek stats SETELAH restart — harus sama..."
	curl -s http://localhost:8080/stats | python -m json.tool
	@echo ""
	@echo "✅  DEMO PERSISTENSI SELESAI"

# ── Segmen 9 — Logs ──

demo-logs-agg:
	docker compose logs aggregator --tail 20

demo-volumes:
	@echo "📦 Menampilkan named volume..."
	docker volume ls | grep -E "pg_data|broker_data"

# ── Segmen 10 — Verifikasi ──

demo-verify:
	@echo "═══════════════════════════════════════"
	@echo " 🏁  VERIFIKASI 100/100 — SCRIPT VIDEO Segmen 10"
	@echo "═══════════════════════════════════════"
	@echo ""
	bash verify.sh

# ── Bonus ──

demo-cleanall:
	@echo "🧹 Membersihkan semua container + volume..."
	docker compose down -v
	@echo ""
	@echo "🧹 Membersihkan file sementara..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov *.egg-info
	@echo "✅  Bersih semua"
