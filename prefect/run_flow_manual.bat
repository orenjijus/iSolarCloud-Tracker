@echo off
REM Manual Flow Execution Script
REM This script runs the MMSR daily pipeline flow manually (for testing)

echo Running MMSR Daily Pipeline Flow...
echo.

REM Change to prefect directory
cd /d "%~dp0"

REM Activate virtual environment if exists
if exist "..\venv\Scripts\activate.bat" (
    call ..\venv\Scripts\activate.bat
)

REM Run the flow
python flows\mmsr_daily_pipeline.py

if errorlevel 1 (
    echo.
    echo ERROR: Flow execution failed
    pause
    exit /b 1
)

echo.
echo Flow execution completed successfully!
pause

