@echo off
REM Hidden Valley hourly pilot — Windows Task Scheduler: "MMSR HV Hourly Pilot"
cd /d "%~dp0\.."

set PYTHON=
if exist ".venv\Scripts\python.exe" set PYTHON=.venv\Scripts\python.exe
if not defined PYTHON set PYTHON=python

"%PYTHON%" "%~dp0run_hv_pilot_cron.py"
set EXITCODE=%ERRORLEVEL%

REM 3 = skipped (01:00-04:59 WIB, jam 1-4) — treat as success for scheduler
if %EXITCODE%==3 exit /b 0
exit /b %EXITCODE%
