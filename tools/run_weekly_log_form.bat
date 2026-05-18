@echo off
REM Jalankan form yang sama (Cleaning Log + Weekly Log) — port 8503 agar tidak bentrok dengan 8501
REM Dari root project: tools\run_weekly_log_form.bat

cd /d "%~dp0"
cd ..
echo Menjalankan form (Cleaning Log + Weekly Log)...
echo Buka browser di http://localhost:8503 — pilih tab Cleaning Log atau Weekly Log
echo.
streamlit run tools/cleaning_log_form.py --server.port 8503
pause
