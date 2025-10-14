@echo off
title ModbusMonitor - Service Manager
echo ========================================
echo ModbusMonitor Service Restart Manager
echo ========================================
echo.
echo This will:
echo 1. Kill all existing Python services
echo 2. Restart all ModbusMonitor services
echo.
echo Press any key to continue or Ctrl+C to cancel...
pause >nul

echo.
echo ========================================
echo Step 1: Stopping all Python processes
echo ========================================

:: Kill all python processes (be careful with this!)
echo Killing Python processes...
taskkill /f /im python.exe 2>nul
taskkill /f /im pythonw.exe 2>nul

:: Wait a moment for processes to terminate
timeout /t 2 /nobreak >nul

echo Python processes terminated.

echo.
echo ========================================
echo Step 2: Starting ModbusMonitor Services
echo ========================================

:: Change to project directory
cd /d "%~dp0"

:: Activate virtual environment if exists
if exist .venv\Scripts\activate.bat (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
)

echo.
echo Starting services in new windows...

:: Start webapp
echo Starting Webapp Service...
start "ModbusMonitor - Webapp" cmd /k "call start_webapp.bat"
timeout /t 2 /nobreak >nul

:: Start orchestra modbus
echo Starting Orchestra Modbus Service...
start "ModbusMonitor - Orchestra Modbus" cmd /k "call start_orchestra_modbus.bat"
timeout /t 2 /nobreak >nul

:: Start alarm worker
echo Starting Alarm Worker Service...
start "ModbusMonitor - Alarm Worker" cmd /k "call start_alarm_worker.bat"
timeout /t 2 /nobreak >nul

:: Start datalogger
echo Starting Datalogger Service...
start "ModbusMonitor - Datalogger" cmd /k "call start_datalogger.bat"
timeout /t 2 /nobreak >nul

echo.
echo ========================================
echo All services started!
echo ========================================
echo.
echo Service Windows:
echo 1. ModbusMonitor - Webapp (http://localhost:5000)
echo 2. ModbusMonitor - Orchestra Modbus
echo 3. ModbusMonitor - Alarm Worker  
echo 4. ModbusMonitor - Datalogger
echo.
echo To stop all services, close all the opened windows
echo or run this script again to restart.
echo.

pause