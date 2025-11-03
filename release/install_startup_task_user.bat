@echo off
setlocal

REM Create a Scheduled Task to start Modbus Monitor at user logon with a 30-second delay.
REM Run this script as the user who should start the app (no admin required for per-user tasks).

set "TASK_NAME=ModbusMonitor AutoStart"
set "SCRIPT_DIR=%~dp0"
set "START_SCRIPT=%SCRIPT_DIR%start_modbus_monitor.bat"

if not exist "%START_SCRIPT%" (
  echo ERROR: start_modbus_monitor.bat not found at "%START_SCRIPT%"
  exit /b 1
)

REM Use ONLOGON so windows is fully loaded for the user session; delay ensures dependent services are up.
REM /RL HIGHEST requests highest privileges in the user session.
REM /F forces update if the task already exists.

schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "\"%START_SCRIPT%\"" ^
  /sc ONLOGON ^
  /rl HIGHEST ^
  /delay 00:30 ^
  /F

if %errorlevel% equ 0 (
  echo Created/Updated task "%TASK_NAME%" to run on user logon with 30s delay.
) else (
  echo Failed to create task. Try running this script as Administrator.
)

endlocal
