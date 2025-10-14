@echo off
title ModbusMonitor - Alarm Worker Service
echo ========================================
echo Starting Alarm Worker Service
echo ========================================
echo.
echo This service handles alarm notifications
echo.
echo Press Ctrl+C to stop the service
echo ========================================

:: Activate virtual environment if exists
if exist .venv\Scripts\activate.bat (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
)

:: Change to project directory
cd /d "%~dp0"

:: Start alarm worker
echo Starting alarm worker service...
python run_alarm_worker.py

pause