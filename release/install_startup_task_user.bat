@echo off
setlocal

REM ============================================================
REM  Install a Scheduled Task that auto-starts Modbus Monitor
REM  on EVERY computer boot / restart (at user logon).
REM
REM  Trigger: ONLOGON  -> runs each time you sign in after a
REM  boot or restart, in your interactive desktop session so the
REM  app windows are visible. (ONSTART runs as SYSTEM in Session 0
REM  with no visible windows, which does not suit this GUI app.)
REM
REM  Just double-click this file. It will request Administrator
REM  rights so the task is registered reliably with HIGHEST run
REM  level; the task itself still runs as the current user.
REM ============================================================

REM --- Self-elevate to Administrator if needed ---
net session >nul 2>&1
if %errorlevel% neq 0 (
  echo Requesting Administrator privileges...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)

set "TASK_NAME=ModbusMonitor AutoStart"
set "SCRIPT_DIR=%~dp0"
set "START_SCRIPT=%SCRIPT_DIR%start_modbus_monitor.bat"
set "RUN_AS=%USERDOMAIN%\%USERNAME%"

if not exist "%START_SCRIPT%" (
  echo ERROR: start_modbus_monitor.bat not found at "%START_SCRIPT%"
  pause
  exit /b 1
)

echo Creating scheduled task "%TASK_NAME%"
echo   Run script : %START_SCRIPT%
echo   Run as     : %RUN_AS%
echo   Trigger    : At logon (every boot / restart), 30s delay
echo.

REM /sc ONLOGON         -> run on every logon (i.e. each boot/restart)
REM /ru "%RUN_AS%" /it  -> run as the current user, interactively (windows visible)
REM /delay 0000:30      -> wait 30s after logon so network/MySQL are ready
REM /rl HIGHEST         -> run with highest privileges
REM /f                  -> overwrite/update if the task already exists
schtasks /create ^
  /tn "%TASK_NAME%" ^
  /sc ONLOGON ^
  /ru "%RUN_AS%" ^
  /it ^
  /delay 0000:30 ^
  /rl HIGHEST ^
  /tr "cmd /c \"%START_SCRIPT%\"" ^
  /f

if %errorlevel% equ 0 (
  echo.
  echo SUCCESS: Task "%TASK_NAME%" created/updated.
  echo It will auto-start Modbus Monitor on every boot/restart at logon.
) else (
  echo.
  echo FAILED to create the task. See the message above.
)

echo.
pause
endlocal
