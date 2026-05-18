@echo off
REM MMSR Daily Pipeline - Windows Batch Script
REM This script runs the daily pipeline and can be scheduled with Windows Task Scheduler

REM Change to script directory
cd /d "%~dp0"

REM Activate virtual environment if it exists (uncomment and adjust path if needed)
REM call venv\Scripts\activate.bat

REM Run the Python script
python run_daily_pipeline.py

REM Check exit code
if %ERRORLEVEL% NEQ 0 (
    echo Pipeline failed with exit code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)

echo Pipeline completed successfully
exit /b 0

