@echo off
setlocal

echo ==============================================
echo   Modbus Monitor - Stop All Services
echo ==============================================

REM Stop WebApp and workers by executable name
for %%P in (modbus_monitor_webserver.exe orchestra_modbus.exe orchestra_datalogger.exe orchestra_alarm.exe) do (
  echo Stopping %%P ...
  taskkill /IM %%P /F /T >nul 2>&1
  if errorlevel 1 (
    echo   - %%P not running or already closed.
  ) else (
    echo   - %%P terminated.
  )
)

echo.
echo All matching processes have been requested to stop.
endlocal
exit /B 0
