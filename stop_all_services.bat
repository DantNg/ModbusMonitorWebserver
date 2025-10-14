@echo off
title ModbusMonitor - Stop All Services
echo ========================================
echo Stopping All ModbusMonitor Services
echo ========================================
echo.
echo This will terminate all Python processes.
echo Make sure you have saved any important work!
echo.
echo Press any key to continue or Ctrl+C to cancel...
pause >nul

echo.
echo Stopping Python services...

:: Kill all python processes
taskkill /f /im python.exe 2>nul
if %errorlevel% == 0 (
    echo ✓ Python.exe processes terminated
) else (
    echo ℹ No python.exe processes found
)

taskkill /f /im pythonw.exe 2>nul
if %errorlevel% == 0 (
    echo ✓ Pythonw.exe processes terminated
) else (
    echo ℹ No pythonw.exe processes found
)

:: Also try to close specific windows by title
taskkill /f /fi "WINDOWTITLE:ModbusMonitor - Webapp*" 2>nul
taskkill /f /fi "WINDOWTITLE:ModbusMonitor - Orchestra*" 2>nul
taskkill /f /fi "WINDOWTITLE:ModbusMonitor - Alarm*" 2>nul
taskkill /f /fi "WINDOWTITLE:ModbusMonitor - Datalogger*" 2>nul

echo.
echo ========================================
echo All services stopped!
echo ========================================

timeout /t 3 /nobreak >nul