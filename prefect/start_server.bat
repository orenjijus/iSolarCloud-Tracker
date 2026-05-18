@echo off
REM Prefect Server Startup Script for Windows
REM This script starts the Prefect server for local development

echo Starting Prefect Server...
echo.

REM Change to prefect directory
cd /d "%~dp0"

REM Activate virtual environment if exists
if exist "..\venv\Scripts\activate.bat" (
    call ..\venv\Scripts\activate.bat
)

REM Start Prefect server
python -m prefect server start --host 0.0.0.0 --port 4200

pause

