@echo off
title ModbusMonitor - Orchestra Datalogger Service
echo ========================================
echo Starting Orchestra Datalogger Service
echo ========================================
echo.
echo This service handles data logging to database
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

:: Start orchestra datalogger
echo Starting orchestra datalogger service...
python orchestra_datalogger.py

pause