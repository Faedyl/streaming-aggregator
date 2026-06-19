#!/usr/bin/env bash
# capture_screenshots.sh — (DISABLED) Screenshot capture tidak diaktifkan.
# Script ini dipertahankan sebagai referensi daftar screenshot yang digunakan
# dalam laporan (report.md). Screenshot diambil secara manual dan sudah
# tersedia di direktori screenshots/.
set -euo pipefail

echo "=== Screenshot capture disabled ==="
echo ""
echo "Screenshots berikut digunakan dalam report.md:"
echo ""
echo "  01 - docker compose ps"
echo "  02 - GET /healthz"
echo "  03 - Publish event baru"
echo "  04 - Publish duplikat"
echo "  05 - Stats baseline"
echo "  06 - Stats setelah load test"
echo "  07 - GET /events"
echo "  08 - pytest results"
echo "  09 - Verify score"
echo "  10 - k6 load test summary"
echo "  11 - Persistence proof"
echo "  12 - Concurrency test"
echo ""
echo "Semua file PNG tersedia di: screenshots/"
echo "Lihat report.md untuk detail penggunaan."
