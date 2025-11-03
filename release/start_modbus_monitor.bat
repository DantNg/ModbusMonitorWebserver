@echo off
setlocal ENABLEDELAYEDEXPANSION

REM Change to the folder of this script
set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%"

echo ==============================================
echo   Modbus Monitor - Start All Services
echo   Folder: %SCRIPT_DIR%
echo ==============================================

REM Point config explicitly (optional but robust)
set "SMTP_CONFIG_PATH=%SCRIPT_DIR%config\SMTP_config.json"
if exist "%SMTP_CONFIG_PATH%" (
	echo Using SMTP_CONFIG_PATH=%SMTP_CONFIG_PATH%
) else (
	echo Warning: %SMTP_CONFIG_PATH% not found. App will try exe dir fallbacks.
)

REM Start WebApp (main.exe)
if exist "main.exe" (
	echo Starting webapp: main.exe
	start "webapp" "%SCRIPT_DIR%main.exe"
) else (
	echo main.exe not found in %SCRIPT_DIR%
)

REM Start Orchestra Modbus worker
if exist "orchestra_modbus.exe" (
	echo Starting worker: orchestra_modbus.exe
	start "orchestra_modbus" "%SCRIPT_DIR%orchestra_modbus.exe"
) else (
	echo orchestra_modbus.exe not found in %SCRIPT_DIR%
)

REM Start Datalogger worker
if exist "orchestra_datalogger.exe" (
	echo Starting worker: orchestra_datalogger.exe
	start "orchestra_datalogger" "%SCRIPT_DIR%orchestra_datalogger.exe"
) else (
	echo orchestra_datalogger.exe not found in %SCRIPT_DIR%
)

REM Start Alarm worker
if exist "orchestra_alarm.exe" (
	echo Starting worker: orchestra_alarm.exe
	start "orchestra_alarm" "%SCRIPT_DIR%orchestra_alarm.exe"
) else (
	echo orchestra_alarm.exe not found in %SCRIPT_DIR%
)

echo.
echo All available services have been started (if found).
echo You can close this window.

popd
endlocal
exit /B 0

