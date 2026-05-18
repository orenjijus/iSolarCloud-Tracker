@echo off
REM Prefect Worker Startup Script for Windows
REM This script starts a Prefect worker to process flow runs

echo Starting Prefect Worker...
echo.

REM Change to prefect directory
cd /d "%~dp0"

REM Activate virtual environment if exists
if exist "..\venv\Scripts\activate.bat" (
    call ..\venv\Scripts\activate.bat
)

REM Start Prefect worker
python -m prefect worker start --pool mmsr-queue

pause

