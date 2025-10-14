@echo off
title ModbusMonitor - Start All Services
echo ========================================
echo Starting All ModbusMonitor Services
echo ========================================
echo.
echo This will start all services in separate windows:
echo 1. Webapp Service (Port 5000)
echo 2. Orchestra Modbus Service
echo 3. Alarm Worker Service
echo 4. Datalogger Service
echo.

:: Change to project directory
cd /d "%~dp0"

:: Activate virtual environment if exists
if exist .venv\Scripts\activate.bat (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
)

echo Starting services...
echo.

:: Start webapp
echo [1/4] Starting Webapp Service...
start "ModbusMonitor - Webapp" cmd /k "call start_webapp.bat"
timeout /t 2 /nobreak >nul

:: Start orchestra modbus
echo [2/4] Starting Orchestra Modbus Service...
start "ModbusMonitor - Orchestra Modbus" cmd /k "call start_orchestra_modbus.bat"
timeout /t 2 /nobreak >nul

:: Start alarm worker
echo [3/4] Starting Alarm Worker Service...
start "ModbusMonitor - Alarm Worker" cmd /k "call start_alarm_worker.bat"
timeout /t 2 /nobreak >nul

:: Start datalogger
echo [4/4] Starting Datalogger Service...
start "ModbusMonitor - Datalogger" cmd /k "call start_datalogger.bat"
timeout /t 2 /nobreak >nul

echo.
echo ========================================
echo All services started successfully!
echo ========================================
echo.
echo You can now access the web interface at:
echo http://localhost:5000
echo.
echo Service windows have been opened.
echo Close this window or press any key to exit.
echo.

pause