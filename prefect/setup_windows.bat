@echo off
REM Prefect Setup Script for Windows Server
REM This script sets up Prefect for MMSR daily pipeline

echo ========================================
echo MMSR Prefect Setup for Windows Server
echo ========================================
echo.

REM Change to prefect directory
cd /d "%~dp0"

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ and add it to PATH
    pause
    exit /b 1
)

echo [1/5] Installing Prefect and dependencies...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

echo.
echo [2/5] Checking environment variables...
if "%POSTGRES_HOST%"=="" (
    echo WARNING: POSTGRES_HOST not set
)
if "%FUSIONSOLAR_USERNAME%"=="" (
    echo WARNING: FUSIONSOLAR_USERNAME not set
)
if "%ISOLARCLOUD_USERNAME%"=="" (
    echo WARNING: ISOLARCLOUD_USERNAME not set
)

echo.
echo [3/5] Testing Prefect installation...
python -c "import prefect; print(f'Prefect version: {prefect.__version__}')"

if errorlevel 1 (
    echo ERROR: Prefect installation failed
    pause
    exit /b 1
)

echo.
echo [4/5] Creating work queue...
python -m prefect work-queue create mmsr-queue --limit 1

echo.
echo [5/5] Setup complete!
echo.
echo Next steps:
echo 1. Set environment variables (POSTGRES_HOST, API credentials, etc.)
echo 2. Start Prefect server: start_server.bat
echo 3. Start Prefect worker: start_worker.bat
echo 4. Deploy the flow: python deploy.py
echo.
echo For production, consider:
echo - Setting up Prefect as Windows Service using NSSM
echo - Using Prefect Cloud for better monitoring
echo.
pause

