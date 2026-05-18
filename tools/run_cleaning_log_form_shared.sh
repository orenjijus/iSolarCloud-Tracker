#!/usr/bin/env bash
# Form Cleaning Log — akses dari LAN. Port 8502 agar tidak bentrok dengan 8501.
# Tim buka http://IP_SERVER:8502

cd "$(dirname "$0")/.."
echo "Cleaning Log Form (shared) - akses dari LAN: http://$(hostname -I 2>/dev/null | awk '{print $1}'):8502"
exec streamlit run tools/cleaning_log_form.py --server.port 8502 --server.address 0.0.0.0
