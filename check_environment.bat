@echo off
title ModbusMonitor - Environment Check
echo ========================================
echo ModbusMonitor Environment Check
echo ========================================
echo.

:: Change to project directory
cd /d "%~dp0"

echo Checking system requirements...
echo.

:: Check Python
echo [1] Python Installation:
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python not found in PATH
    echo    Please install Python 3.8+ from https://python.org/downloads/
) else (
    python --version
    echo ✓ Python found
)

echo.

:: Check virtual environment
echo [2] Virtual Environment:
if exist .venv\Scripts\activate.bat (
    echo ✓ Virtual environment found at .venv
    
    :: Check if it's working
    call .venv\Scripts\activate.bat >nul 2>&1
    if "%VIRTUAL_ENV%"=="" (
        echo ❌ Virtual environment exists but cannot be activated
        echo    Try running: setup_venv.bat
    ) else (
        echo ✓ Virtual environment can be activated
        
        :: Check key packages
        echo.
        echo [3] Key Packages:
        python -c "import flask; print('✓ Flask:', flask.__version__)" 2>nul || echo "❌ Flask missing"
        python -c "import flask_socketio; print('✓ Flask-SocketIO')" 2>nul || echo "❌ Flask-SocketIO missing"
        python -c "import sqlalchemy; print('✓ SQLAlchemy:', sqlalchemy.__version__)" 2>nul || echo "❌ SQLAlchemy missing"
        python -c "import pymodbus; print('✓ PyModbus:', pymodbus.__version__)" 2>nul || echo "❌ PyModbus missing"
        python -c "import pymysql; print('✓ PyMySQL')" 2>nul || echo "❌ PyMySQL missing"
        
        :: Check project modules
        echo.
        echo [4] Project Modules:
        python -c "import sys; sys.path.append('webapp'); import modbus_monitor; print('✓ modbus_monitor module')" 2>nul || echo "❌ modbus_monitor module import failed"
        python -c "import workers.tcp_worker; print('✓ tcp_worker module')" 2>nul || echo "❌ tcp_worker module import failed"
        python -c "import workers.rtu_worker; print('✓ rtu_worker module')" 2>nul || echo "❌ rtu_worker module import failed"
    )
) else (
    echo ❌ Virtual environment not found
    echo    Run: setup_venv.bat to create it
)

echo.

:: Check configuration files
echo [5] Configuration Files:
if exist config\SMTP_config.json (
    echo ✓ config\SMTP_config.json found
) else (
    echo ❌ config\SMTP_config.json missing
)

if exist requirements.txt (
    echo ✓ requirements.txt found
) else (
    echo ⚠️  requirements.txt missing (optional)
)

echo.

:: Check database files
echo [6] Database Files:
set db_found=0
for %%f in (*.db) do (
    echo ✓ Database file: %%f
    set db_found=1
)
if %db_found%==0 (
    echo ⚠️  No .db files found (will be created automatically)
)

echo.

:: Check scripts
echo [7] Service Scripts:
if exist start_all_services.bat (echo ✓ start_all_services.bat) else (echo ❌ start_all_services.bat missing)
if exist stop_all_services.bat (echo ✓ stop_all_services.bat) else (echo ❌ stop_all_services.bat missing)
if exist start_webapp.bat (echo ✓ start_webapp.bat) else (echo ❌ start_webapp.bat missing)

echo.
echo ========================================
echo Diagnosis Summary
echo ========================================

:: Count issues
set issues=0

python --version >nul 2>&1 || set /a issues+=1
if not exist .venv\Scripts\activate.bat set /a issues+=1
if not exist config\SMTP_config.json set /a issues+=1
if not exist start_all_services.bat set /a issues+=1

if %issues%==0 (
    echo.
    echo 🎉 All checks passed! Environment is ready.
    echo.
    echo You can now run:
    echo   start_all_services.bat
    echo.
) else (
    echo.
    echo ⚠️  Found %issues% issue(s) that need attention.
    echo.
    echo Recommended actions:
    if not exist .venv\Scripts\activate.bat echo • Run: setup_venv.bat
    if not exist config\SMTP_config.json echo • Create: config\SMTP_config.json
    if not exist start_all_services.bat echo • Re-run the setup scripts
    echo.
)

echo Press any key to exit...
pause >nul