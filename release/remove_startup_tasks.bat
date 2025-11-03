@echo off
setlocal

REM Remove both user and system scheduled tasks installed by helper scripts.

set "TASK_USER=ModbusMonitor AutoStart"
set "TASK_SYSTEM=ModbusMonitor AutoStart (System)"

schtasks /delete /tn "%TASK_USER%" /f >nul 2>&1
if %errorlevel% equ 0 ( echo Removed task: %TASK_USER% ) else ( echo Task not found or already removed: %TASK_USER% )

schtasks /delete /tn "%TASK_SYSTEM%" /f >nul 2>&1
if %errorlevel% equ 0 ( echo Removed task: %TASK_SYSTEM% ) else ( echo Task not found or already removed: %TASK_SYSTEM% )

endlocal
