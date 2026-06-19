#!/usr/bin/env bash
# verify.sh — jalankan: bash verify.sh
# Return 0 = skor 100, Return 1 = skor < 100
set -u

SCORE=0
MAX=100

ok()   { echo "  ✅  $1  (+$2)"; SCORE=$((SCORE + $2)); }
fail() { echo "  ❌  $1"; }

echo "======================================================"
echo " VERIFIKASI UAS SISTEM TERDISTRIBUSI"
echo " $(date)"
echo "======================================================"

# ─────────────────────────────────────────────────────────
# 1. STRUKTUR FILE (15)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ STRUKTUR FILE ]"

[ -f docker-compose.yml ]             && ok "docker-compose.yml"             2 || fail "docker-compose.yml HILANG"
[ -f aggregator/Dockerfile ]          && ok "aggregator/Dockerfile"          2 || fail "aggregator/Dockerfile HILANG"
[ -f publisher/Dockerfile ]           && ok "publisher/Dockerfile"           2 || fail "publisher/Dockerfile HILANG"
[ -f aggregator/requirements.txt ]    && ok "aggregator/requirements.txt"    1 || fail "requirements.txt HILANG"
[ -f aggregator/app/main.py ]         && ok "aggregator/app/main.py"         2 || fail "main.py HILANG"
[ -f aggregator/app/dedup.py ]        && ok "aggregator/app/dedup.py"        2 || fail "dedup.py HILANG"
[ -f aggregator/app/consumer.py ]     && ok "aggregator/app/consumer.py"     2 || fail "consumer.py HILANG"
[ -f aggregator/app/routes.py ]       && ok "aggregator/app/routes.py"       1 || fail "routes.py HILANG"
[ -f publisher/app.py ]               && ok "publisher/app.py"               1 || fail "publisher/app.py HILANG"

# ─────────────────────────────────────────────────────────
# 2. TESTS (10)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ TESTS ]"

[ -d aggregator/tests ]                && ok "folder tests/ ada"         2 || fail "folder tests/ hilang"
N_TESTS=$(find aggregator/tests -name "test_*.py" 2>/dev/null | wc -l | tr -d ' ')
[ "${N_TESTS:-0}" -ge 5 ]              && ok "${N_TESTS} file test ditemukan" 4 || fail "kurang file test (ada ${N_TESTS:-0})"
[ -f aggregator/tests/conftest.py ]    && ok "conftest.py ada"           2 || fail "conftest.py hilang"
[ -f k6/loadtest.js ]                  && ok "k6/loadtest.js ada"        2 || fail "k6/loadtest.js hilang"

# ─────────────────────────────────────────────────────────
# 3. COMPOSE SYNTAX (5)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ COMPOSE SYNTAX ]"

docker compose config -q 2>/dev/null  && ok "compose config valid"      5 || fail "compose INVALID (jalankan: docker compose config)"

# ─────────────────────────────────────────────────────────
# 4. BUILD & START SERVICES (15)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ BUILD & START ]"
echo "  ... menjalankan docker compose down lalu up --build ..."

DOCKER_BUILDKIT=0 docker compose down --remove-orphans -v > /tmp/_compose.log 2>&1
DOCKER_BUILDKIT=0 docker compose up -d --build           >> /tmp/_compose.log 2>&1

echo "  ... menunggu 20 detik agar services ready ..."
sleep 20

AGG_STATE=$(docker compose ps --format json 2>/dev/null | python3 -c "
import sys,json
data = [json.loads(l) for l in sys.stdin if l.strip()]
for s in data:
    if 'aggregator' in s.get('Service','') and 'load' not in s.get('Service',''):
        print(s.get('State','unknown')); break
" 2>/dev/null || echo "unknown")

PG_STATE=$(docker compose ps --format json 2>/dev/null | python3 -c "
import sys,json
data = [json.loads(l) for l in sys.stdin if l.strip()]
for s in data:
    if 'storage' in s.get('Service',''):
        print(s.get('State','unknown')); break
" 2>/dev/null || echo "unknown")

REDIS_STATE=$(docker compose ps --format json 2>/dev/null | python3 -c "
import sys,json
data = [json.loads(l) for l in sys.stdin if l.strip()]
for s in data:
    if 'broker' in s.get('Service',''):
        print(s.get('State','unknown')); break
" 2>/dev/null || echo "unknown")

[ "$AGG_STATE" = "running" ]   && ok "aggregator: running"   5 || fail "aggregator: $AGG_STATE (lihat: docker compose logs aggregator)"
[ "$PG_STATE"  = "running" ]   && ok "postgres: running"     5 || fail "postgres: $PG_STATE"
[ "$REDIS_STATE" = "running" ] && ok "redis: running"         5 || fail "redis: $REDIS_STATE"

# ─────────────────────────────────────────────────────────
# 5. HEALTHZ (5)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ HEALTHZ ]"

HEALTH=$(curl -sf http://localhost:8080/healthz 2>/dev/null || echo '{"status":"error"}')
echo "  response: $HEALTH"
echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); exit(0 if d.get('status')=='ok' else 1)" 2>/dev/null \
  && ok "GET /healthz → status:ok" 5 || fail "GET /healthz gagal: $HEALTH"

# ─────────────────────────────────────────────────────────
# 6. IDEMPOTENCY & DEDUP (10)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ IDEMPOTENCY & DEDUP ]"

EVT='{"topic":"verify-dedup","event_id":"VERIFY-001-FIXED","timestamp":"2025-01-15T10:00:00Z","source":"verify.sh","payload":{"check":true}}'

curl -sf -X POST http://localhost:8080/publish -H 'Content-Type: application/json' -d "$EVT" > /dev/null 2>&1
sleep 2
curl -sf -X POST http://localhost:8080/publish -H 'Content-Type: application/json' -d "$EVT" > /dev/null 2>&1
sleep 2
curl -sf -X POST http://localhost:8080/publish -H 'Content-Type: application/json' -d "$EVT" > /dev/null 2>&1
sleep 3  # beri waktu consumer memproses

STATS=$(curl -sf http://localhost:8080/stats 2>/dev/null || echo '{}')
echo "  /stats: $STATS"

UNIQ=$(echo "$STATS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('unique_processed',0))" 2>/dev/null || echo 0)
DUP=$(echo  "$STATS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('duplicate_dropped',0))" 2>/dev/null || echo 0)

[ "${UNIQ:-0}" -ge 1 ] && ok "unique_processed ≥ 1 (nilai: $UNIQ)"  5 || fail "unique_processed = $UNIQ (consumer mungkin belum proses)"
[ "${DUP:-0}"  -ge 2 ] && ok "duplicate_dropped ≥ 2 (nilai: $DUP)"  5 || fail "duplicate_dropped = $DUP (dedup mungkin belum jalan)"

# ─────────────────────────────────────────────────────────
# 7. PERSISTENSI (10)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ PERSISTENSI ]"

UNIQ_BEFORE="$UNIQ"
echo "  ... stop & start aggregator (tanpa hapus volume) ..."

docker compose stop aggregator > /dev/null 2>&1
sleep 3
docker compose start aggregator > /dev/null 2>&1
sleep 10

STATS2=$(curl -sf http://localhost:8080/stats 2>/dev/null || echo '{}')
UNIQ2=$(echo "$STATS2" | python3 -c "import sys,json; print(json.load(sys.stdin).get('unique_processed',0))" 2>/dev/null || echo 0)
echo "  before=$UNIQ_BEFORE after=$UNIQ2"

[ "${UNIQ2:-0}" -ge "${UNIQ_BEFORE:-1}" ] && ok "data persist setelah restart (${UNIQ_BEFORE}→${UNIQ2})" 10 \
  || fail "data HILANG setelah restart! before=$UNIQ_BEFORE after=$UNIQ2"

# ─────────────────────────────────────────────────────────
# 8. VOLUMES (5)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ VOLUMES ]"

docker volume ls 2>/dev/null | grep -q "pg_data"      && ok "volume pg_data ada"      3 || fail "volume pg_data TIDAK ADA"
docker volume ls 2>/dev/null | grep -q "broker_data"  && ok "volume broker_data ada"   2 || fail "volume broker_data TIDAK ADA"

# ─────────────────────────────────────────────────────────
# 9. DOCKERFILE NON-ROOT (5)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ DOCKERFILE NON-ROOT ]"

grep -q "USER app\|USER 1000\|USER 1001" aggregator/Dockerfile 2>/dev/null \
  && ok "aggregator Dockerfile: non-root user"  3 || fail "aggregator Dockerfile: root user terdeteksi!"
grep -q "USER app\|USER 1000\|USER 1001" publisher/Dockerfile  2>/dev/null \
  && ok "publisher Dockerfile: non-root user"   2 || fail "publisher Dockerfile: root user terdeteksi!"

# ─────────────────────────────────────────────────────────
# 10. DOKUMENTASI (5)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ DOKUMENTASI ]"

[ -f report.md ]  && ok "report.md ada"  3 || fail "report.md HILANG"
[ -f README.md ]  && ok "README.md ada"  2 || fail "README.md HILANG"

# ─────────────────────────────────────────────────────────
# 11. FILE PENDUKUNG & VALIDASI (15)
# ─────────────────────────────────────────────────────────
echo ""
echo "[ FILE PENDUKUNG & VALIDASI ]"

[ -f .env.example ]       && ok ".env.example"       2 || fail ".env.example HILANG"
[ -f .gitignore ]          && ok ".gitignore"          2 || fail ".gitignore HILANG"
grep -q "topic\|event_id" aggregator/app/models.py    2>/dev/null && ok "models.py field lengkap"  3 || fail "models.py tidak lengkap"
grep -q "schema.sql" aggregator/app/db.py              2>/dev/null && ok "db.py execute schema.sql" 3 || fail "db.js tidak execute schema.sql"
[ -f aggregator/app/config.py ]       && ok "config.py ada"        2 || fail "config.py HILANG"
[ -f aggregator/app/models.py ]       && ok "models.py ada"        2 || fail "models.py HILANG"
[ -f aggregator/app/schema.sql ]      && ok "schema.sql ada"       1 || fail "schema.sql HILANG"

# ─────────────────────────────────────────────────────────
# HASIL AKHIR
# ─────────────────────────────────────────────────────────
echo ""
echo "======================================================"
printf " SKOR AKHIR: %d / %d\n" "$SCORE" "$MAX"
echo "======================================================"

if [ "$SCORE" -eq "$MAX" ]; then
  echo " 🎉  SEMPURNA! Semua cek lulus. Siap submit."
  exit 0
else
  SISA=$((MAX - SCORE))
  echo " ⚠️   Masih kurang $SISA poin. Perbaiki bagian yang ❌ lalu ulangi."
  exit 1
fi
