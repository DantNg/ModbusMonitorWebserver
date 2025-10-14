@echo off
title ModbusMonitor - Orchestra Modbus Service
echo ========================================
echo Starting Orchestra Modbus Service
echo ========================================
echo.
echo This service manages TCP/RTU Modbus workers
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

:: Start orchestra modbus
echo Starting orchestra modbus service...
python orchestra_modbus.py

pause