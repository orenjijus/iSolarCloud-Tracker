@echo off
REM Jalankan form Cleaning Log (untuk tim engineering)
REM Dari folder tools: run_cleaning_log_form.bat
REM Dari root project: tools\run_cleaning_log_form.bat

cd /d "%~dp0"
cd ..
echo Menjalankan Cleaning Log Form...
echo Buka browser di http://localhost:8501
echo Untuk akses dari komputer lain di LAN: pakai run_cleaning_log_form_shared.bat
echo.
streamlit run tools/cleaning_log_form.py --server.port 8501
pause
