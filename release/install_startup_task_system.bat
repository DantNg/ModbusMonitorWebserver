@echo off
setlocal

REM Create a Scheduled Task to start Modbus Monitor at system startup with a 30-second delay.
REM Requires Administrator privileges. Runs in the background even before any user logs in.

set "TASK_NAME=ModbusMonitor AutoStart (System)"
set "SCRIPT_DIR=%~dp0"
set "START_SCRIPT=%SCRIPT_DIR%start_modbus_monitor.bat"

if not exist "%START_SCRIPT" (
  echo ERROR: start_modbus_monitor.bat not found at "%START_SCRIPT%"
  exit /b 1
)

REM ONSTART ensures the task runs when the OS boots. It runs under LocalSystem so no desktop UI will show.
REM /RL HIGHEST gives full privileges; /RU SYSTEM sets the account; /F overwrites existing.

REM Use embedded delay and ensure working directory; capture task output to task.log for diagnostics
schtasks /create ^
  /tn "%TASK_NAME%" ^
  /sc ONSTART ^
  /RU "SYSTEM" ^
  /RL HIGHEST ^
  /tr "cmd /c cd /d \"%SCRIPT_DIR%\" ^&^& timeout /t 30 /nobreak >nul ^&^& \"\"%START_SCRIPT%\"\" >> \"%SCRIPT_DIR%task.log\" 2^>^&1" ^
  /F

if %errorlevel% equ 0 (
  echo Created/Updated system task "%TASK_NAME%" to run on startup with 30s delay.
) else (
  echo Failed to create system task. Run this script as Administrator.
)

endlocal
