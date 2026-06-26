@echo off

REM --- Relaunch this script in a MINIMIZED terminal so the console does not
REM     pop up in front. The "/relaunched" marker prevents an infinite loop. ---
if not "%~1"=="/relaunched" (
    start "Modbus Monitor" /min cmd /c "%~f0" /relaunched
    exit /b
)

setlocal ENABLEDELAYEDEXPANSION

REM Change to the folder of this script
set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%"

echo ==============================================
echo   Modbus Monitor - Start All Services
echo   Folder: %SCRIPT_DIR%
echo ==============================================

REM Ensure logs directory exists and start logging
if not exist "logs" mkdir "logs"
set "LOG=%SCRIPT_DIR%logs\startup.log"
echo [%date% %time%] ===== Start Modbus Monitor =====>>"%LOG%"

REM Optional: wait 3 seconds after Windows fully loads before starting
echo Waiting 3 seconds to ensure system services are ready...
echo [%date% %time%] Waiting 3 seconds before launching services...>>"%LOG%"
timeout /t 3 /nobreak >nul

REM Point config explicitly (optional but robust)
set "SMTP_CONFIG_PATH=%SCRIPT_DIR%config\SMTP_config.json"
if exist "%SMTP_CONFIG_PATH%" (
	echo Using SMTP_CONFIG_PATH=%SMTP_CONFIG_PATH%
	echo [%date% %time%] Using SMTP_CONFIG_PATH=%SMTP_CONFIG_PATH%>>"%LOG%"
) else (
	echo Warning: %SMTP_CONFIG_PATH% not found. App will try exe dir fallbacks.
	echo [%date% %time%] WARNING: %SMTP_CONFIG_PATH% not found. Using exe dir fallbacks.>>"%LOG%"
)

REM Start WebApp (modbus_monitor_webserver.exe)
if exist "modbus_monitor_webserver.exe" (
	echo Starting webapp: modbus_monitor_webserver.exe
	echo [%date% %time%] Starting: modbus_monitor_webserver.exe>>"%LOG%"
	start "webapp" /min "%SCRIPT_DIR%modbus_monitor_webserver.exe"
) else (
	echo modbus_monitor_webserver.exe not found in %SCRIPT_DIR%
	echo [%date% %time%] Skip modbus_monitor_webserver.exe (not found)>>"%LOG%"
)
echo Waiting 5 seconds to ensure system services are ready...
echo [%date% %time%] Waiting 5 seconds before launching services...>>"%LOG%"
timeout /t 5 /nobreak >nul

REM Start Orchestra Modbus worker
if exist "orchestra_modbus.exe" (
	echo Starting worker: orchestra_modbus.exe
	echo [%date% %time%] Starting: orchestra_modbus.exe>>"%LOG%"
	start "orchestra_modbus" /min "%SCRIPT_DIR%orchestra_modbus.exe"
) else (
	echo orchestra_modbus.exe not found in %SCRIPT_DIR%
	echo [%date% %time%] Skip orchestra_modbus.exe (not found)>>"%LOG%"
)

REM Start Datalogger worker
if exist "orchestra_datalogger.exe" (
	echo Starting worker: orchestra_datalogger.exe
	echo [%date% %time%] Starting: orchestra_datalogger.exe>>"%LOG%"
	start "orchestra_datalogger" /min "%SCRIPT_DIR%orchestra_datalogger.exe"
) else (
	echo orchestra_datalogger.exe not found in %SCRIPT_DIR%
	echo [%date% %time%] Skip orchestra_datalogger.exe (not found)>>"%LOG%"
)

echo Waiting 3 seconds to ensure system services are ready...
echo [%date% %time%] Waiting 3 seconds before launching services...>>"%LOG%"
timeout /t 3 /nobreak >nul
	
REM Start Alarm worker
if exist "orchestra_alarm.exe" (
	echo Starting worker: orchestra_alarm.exe
	echo [%date% %time%] Starting: orchestra_alarm.exe>>"%LOG%"
	start "orchestra_alarm" /min "%SCRIPT_DIR%orchestra_alarm.exe"
) else (
	echo orchestra_alarm.exe not found in %SCRIPT_DIR%
	echo [%date% %time%] Skip orchestra_alarm.exe (not found)>>"%LOG%"
)

echo.
echo All available services have been started (if found).
echo You can close this window.

echo [%date% %time%] ===== Done =====>>"%LOG%"

popd
endlocal
exit /B 0

