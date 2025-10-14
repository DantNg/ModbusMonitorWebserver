@echo off
title ModbusMonitor - Stop All Services
echo ========================================
echo Stopping All ModbusMonitor Services
echo ========================================
echo.
echo This will terminate all Python processes.
echo Make sure you have saved any important work!
echo.

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
:: Kill all open Command Prompt windows
taskkill /f /im cmd.exe 2>nul
if %errorlevel% == 0 (
    echo ✓ All Command Prompt windows terminated
) else (
    echo ℹ No Command Prompt windows found
)
timeout /t 3 /nobreak >nul