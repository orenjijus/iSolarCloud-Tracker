#!/usr/bin/env bash
# Jalankan form Cleaning Log (untuk tim engineering)
# Dari root project: ./tools/run_cleaning_log_form.sh
# Atau: bash tools/run_cleaning_log_form.sh

cd "$(dirname "$0")/.."
echo "Menjalankan Cleaning Log Form..."
echo "Buka browser di http://localhost:8501"
echo "Untuk akses dari LAN: ./tools/run_cleaning_log_form_shared.sh"
echo ""
exec streamlit run tools/cleaning_log_form.py --server.port 8501
