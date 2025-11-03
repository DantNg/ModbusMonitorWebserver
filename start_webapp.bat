@echo off
title ModbusMonitor - Webapp Service
echo ========================================
echo Starting ModbusMonitor Webapp
echo ========================================
echo.
echo Web interface will be available at:
echo http://localhost:5000
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

:: Start webapp
echo Starting webapp service...
python main.py

pause