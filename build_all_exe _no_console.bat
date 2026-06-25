@echo off
chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

:: ============================================================================
:: Build All EXE - Auto build all Modbus Monitor services using PyInstaller
:: ============================================================================
echo ============================================================
echo   Modbus Monitor - Auto Build All EXE
echo   Date: %date% %time%
echo ============================================================
echo.

:: --- Configuration ---
set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

:: Activate virtual environment if exists
if exist ".venv\Scripts\activate.bat" (
    echo [INFO] Activating virtual environment...
    call .venv\Scripts\activate.bat
) else (
    echo [WARN] No .venv found, using system Python
)

:: Check PyInstaller is available
where pyinstaller >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] PyInstaller not found! Install it with: pip install pyinstaller
    pause
    exit /b 1
)

:: Create output directory
if not exist "release" mkdir release
if not exist "build" mkdir build

:: Shared hidden imports for all builds
set "HIDDEN_IMPORTS=--hidden-import=eventlet.hubs.epolls"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=eventlet.hubs.kqueue"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=eventlet.hubs.selects"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.rdtypes.ANY"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.rdtypes.ANY.CNAME"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.rdtypes.ANY.MX"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.rdtypes.ANY.NS"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.rdtypes.ANY.PTR"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.rdtypes.ANY.SOA"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.rdtypes.ANY.TXT"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.asyncbackend"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.dnssec"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.e164"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.namedict"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.tsig"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.update"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.versioned"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.zone"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=dns.tsigkeyring"
:: DB driver (loaded dynamically by SQLAlchemy via mysql+pymysql URI)
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=pymysql"
:: Serial / SMS modem support (alarm worker SMS reconnect + RTU worker)
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=serial"
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=serial.tools.list_ports"
:: Modbus client (RTU/TCP)
set "HIDDEN_IMPORTS=%HIDDEN_IMPORTS% --hidden-import=pymodbus"

:: Output directly into release/ (start scripts and summary expect exes there)
set "OUTPUT=--distpath release --workpath build"

:: --- App icons (optional, one per exe) ---
:: To customize an exe icon, put the matching .ico file at the path below
:: (or change the ICON_PATH* value). If a file is not found, PyInstaller's
:: default icon is used for that exe.
::   ICON_PATH1 -> modbus_monitor_webserver.exe
::   ICON_PATH2 -> orchestra_modbus.exe
::   ICON_PATH3 -> orchestra_datalogger.exe
::   ICON_PATH4 -> orchestra_alarm.exe
set "ICON_PATH1=%PROJECT_DIR%webserver.ico"
set "ICON_PATH2=%PROJECT_DIR%modbus.ico"
set "ICON_PATH3=%PROJECT_DIR%datalogger.ico"
set "ICON_PATH4=%PROJECT_DIR%alarm.ico"

set "ICON_OPT1="
set "ICON_OPT2="
set "ICON_OPT3="
set "ICON_OPT4="

if exist "%ICON_PATH1%" (
    echo [INFO] Webserver icon: %ICON_PATH1%
    set ICON_OPT1=--icon="%ICON_PATH1%"
) else (
    echo [INFO] No webserver icon at "%ICON_PATH1%" - using default icon
)
if exist "%ICON_PATH2%" (
    echo [INFO] Modbus icon: %ICON_PATH2%
    set ICON_OPT2=--icon="%ICON_PATH2%"
) else (
    echo [INFO] No modbus icon at "%ICON_PATH2%" - using default icon
)
if exist "%ICON_PATH3%" (
    echo [INFO] Datalogger icon: %ICON_PATH3%
    set ICON_OPT3=--icon="%ICON_PATH3%"
) else (
    echo [INFO] No datalogger icon at "%ICON_PATH3%" - using default icon
)
if exist "%ICON_PATH4%" (
    echo [INFO] Alarm icon: %ICON_PATH4%
    set ICON_OPT4=--icon="%ICON_PATH4%"
) else (
    echo [INFO] No alarm icon at "%ICON_PATH4%" - using default icon
)

:: Shared data files for worker builds
set "DATA_WORKERS=--add-data=workers;workers/"
set "DATA_UTILS=--add-data=utils;utils/"
set "DATA_SHARED=--add-data=shared;shared/"
set "DATA_WEBAPP=--add-data=webapp;webapp/"
set "DATA_CONFIG=--add-data=web_config.txt;."

:: Track build results
set "BUILD_SUCCESS=0"
set "BUILD_FAIL=0"
set "RESULTS="

:: ============================================================================
:: BUILD 1: modbus_monitor_webserver.exe (Main Web Application)
:: ============================================================================
echo.
echo ============================================================
echo [1/4] Building: modbus_monitor_webserver.exe
echo ============================================================
echo.

pyinstaller --noconfirm --onefile --noconsole ^
    %OUTPUT% ^
    %ICON_OPT1% ^
    %HIDDEN_IMPORTS% ^
    --hidden-import=sqlalchemy ^
    --hidden-import=eventlet ^
    --hidden-import=engineio.async_drivers.eventlet ^
    %DATA_WORKERS% ^
    %DATA_UTILS% ^
    %DATA_SHARED% ^
    %DATA_WEBAPP% ^
    %DATA_CONFIG% ^
    modbus_monitor_webserver.py

if %ERRORLEVEL% equ 0 (
    echo [OK] modbus_monitor_webserver.exe built successfully
    set /a BUILD_SUCCESS+=1
    set "RESULTS=!RESULTS!  [OK] modbus_monitor_webserver.exe\n"
) else (
    echo [FAIL] modbus_monitor_webserver.exe build failed!
    set /a BUILD_FAIL+=1
    set "RESULTS=!RESULTS!  [FAIL] modbus_monitor_webserver.exe\n"
)

:: ============================================================================
:: BUILD 2: orchestra_modbus.exe (Modbus Worker Orchestrator)
:: ============================================================================
echo.
echo ============================================================
echo [2/4] Building: orchestra_modbus.exe
echo ============================================================
echo.

pyinstaller --noconfirm --onefile --noconsole ^
    %OUTPUT% ^
    %ICON_OPT2% ^
    %HIDDEN_IMPORTS% ^
    --hidden-import=sqlalchemy ^
    %DATA_WORKERS% ^
    %DATA_UTILS% ^
    %DATA_SHARED% ^
    %DATA_WEBAPP% ^
    %DATA_CONFIG% ^
    orchestra_modbus.py

if %ERRORLEVEL% equ 0 (
    echo [OK] orchestra_modbus.exe built successfully
    set /a BUILD_SUCCESS+=1
    set "RESULTS=!RESULTS!  [OK] orchestra_modbus.exe\n"
) else (
    echo [FAIL] orchestra_modbus.exe build failed!
    set /a BUILD_FAIL+=1
    set "RESULTS=!RESULTS!  [FAIL] orchestra_modbus.exe\n"
)

:: ============================================================================
:: BUILD 3: orchestra_datalogger.exe (Datalogger Orchestrator)
:: ============================================================================
echo.
echo ============================================================
echo [3/4] Building: orchestra_datalogger.exe
echo ============================================================
echo.

pyinstaller --noconfirm --onefile --noconsole ^
    %OUTPUT% ^
    %ICON_OPT3% ^
    %HIDDEN_IMPORTS% ^
    --hidden-import=sqlalchemy ^
    %DATA_WORKERS% ^
    %DATA_UTILS% ^
    %DATA_SHARED% ^
    orchestra_datalogger.py

if %ERRORLEVEL% equ 0 (
    echo [OK] orchestra_datalogger.exe built successfully
    set /a BUILD_SUCCESS+=1
    set "RESULTS=!RESULTS!  [OK] orchestra_datalogger.exe\n"
) else (
    echo [FAIL] orchestra_datalogger.exe build failed!
    set /a BUILD_FAIL+=1
    set "RESULTS=!RESULTS!  [FAIL] orchestra_datalogger.exe\n"
)

:: ============================================================================
:: BUILD 4: orchestra_alarm.exe (Alarm Worker Orchestrator)
:: ============================================================================
echo.
echo ============================================================
echo [4/4] Building: orchestra_alarm.exe
echo ============================================================
echo.

pyinstaller --noconfirm --onefile --noconsole ^
    %OUTPUT% ^
    %ICON_OPT4% ^
    %HIDDEN_IMPORTS% ^
    --hidden-import=sqlalchemy ^
    %DATA_WORKERS% ^
    %DATA_UTILS% ^
    %DATA_SHARED% ^
    %DATA_WEBAPP% ^
    orchestra_alarm.py

if %ERRORLEVEL% equ 0 (
    echo [OK] orchestra_alarm.exe built successfully
    set /a BUILD_SUCCESS+=1
    set "RESULTS=!RESULTS!  [OK] orchestra_alarm.exe\n"
) else (
    echo [FAIL] orchestra_alarm.exe build failed!
    set /a BUILD_FAIL+=1
    set "RESULTS=!RESULTS!  [FAIL] orchestra_alarm.exe\n"
)

:: ============================================================================
:: Copy additional files to release
:: ============================================================================
echo.
echo [INFO] Copying additional files to release...

if exist "config" xcopy /Y /E /I "config" "release\config" >nul 2>&1
if exist "version_info.txt" copy /Y "version_info.txt" "release\version_info.txt" >nul 2>&1


:: ============================================================================
:: Build Summary
:: ============================================================================
echo.
echo ============================================================
echo   BUILD SUMMARY
echo ============================================================
echo   Successful: %BUILD_SUCCESS% / 4
echo   Failed:     %BUILD_FAIL% / 4
echo.
echo   Results:
echo   %RESULTS%
echo.

:: List output files
echo   Output files in release/:
echo   -------------------------
if exist "release\modbus_monitor_webserver.exe" (
    for %%A in ("release\modbus_monitor_webserver.exe") do echo   modbus_monitor_webserver.exe  [%%~zA bytes]
)
if exist "release\orchestra_modbus.exe" (
    for %%A in ("release\orchestra_modbus.exe") do echo   orchestra_modbus.exe           [%%~zA bytes]
)
if exist "release\orchestra_datalogger.exe" (
    for %%A in ("release\orchestra_datalogger.exe") do echo   orchestra_datalogger.exe       [%%~zA bytes]
)
if exist "release\orchestra_alarm.exe" (
    for %%A in ("release\orchestra_alarm.exe") do echo   orchestra_alarm.exe            [%%~zA bytes]
)

echo.
echo ============================================================

if %BUILD_FAIL% gtr 0 (
    echo   [WARN] Some builds failed! Check logs above.
    echo.
    pause
    exit /b 1
)

echo   All builds completed successfully!
echo.
pause
exit /b 0
