@echo off
REM Form Cleaning Log — akses dari device lain (LAN). Port 8502.
REM JANGAN buka http://0.0.0.0:8502 — itu invalid. Gunakan IP asli server.

cd /d "%~dp0"
cd ..

echo ============================================
echo  Cleaning Log Form (shared)
echo ============================================
echo  JANGAN buka 0.0.0.0 — itu invalid.
echo.
echo  Dari device lain: http://[IP_SERVER]:8502
echo  Cek IP server: ketik ipconfig, lihat IPv4 Address
echo  Dari server ini: http://localhost:8502
echo.
echo  Agar jalan 24 jam: jangan tutup jendela ini.
echo ============================================
echo.

streamlit run tools/cleaning_log_form.py --server.port 8502 --server.address 0.0.0.0
pause
