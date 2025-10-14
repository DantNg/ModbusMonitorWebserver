@echo off
title ModbusMonitor - Complete Setup
echo ========================================
echo ModbusMonitor Complete Setup
echo ========================================
echo.
echo This script will:
echo 1. Check Python installation
echo 2. Create virtual environment
echo 3. Install all dependencies
echo 4. Verify installation
echo 5. Create shortcut scripts
echo.
echo Press any key to continue or Ctrl+C to cancel...
pause >nul

:: Change to project directory
cd /d "%~dp0"

echo.
echo ========================================
echo Step 1: Checking Python Installation
echo ========================================

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python is not installed or not in PATH
    echo.
    echo Please install Python 3.8+ from:
    echo https://python.org/downloads/
    echo.
    echo Make sure to check "Add Python to PATH" during installation
    pause
    exit /b 1
)

echo ✓ Python found:
python --version

echo.
echo ========================================
echo Step 2: Creating Virtual Environment
echo ========================================

if exist .venv (
    echo Virtual environment already exists, skipping creation...
) else (
    echo Creating virtual environment...
    python -m venv .venv
    
    if not exist .venv\Scripts\activate.bat (
        echo ❌ Failed to create virtual environment
        pause
        exit /b 1
    )
    
    echo ✓ Virtual environment created
)

echo.
echo ========================================
echo Step 3: Installing Dependencies
echo ========================================

:: Activate virtual environment
call .venv\Scripts\activate.bat

:: Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip

:: Install packages
echo.
echo Installing essential packages...

echo [1/4] Installing Flask framework...
pip install flask flask-socketio eventlet

echo [2/4] Installing database drivers...
pip install sqlalchemy pymysql mysql-connector-python

echo [3/4] Installing Modbus libraries...
pip install pymodbus

echo [4/4] Installing utilities...
pip install python-dotenv requests schedule

:: Install PyInstaller for building exe
echo [Extra] Installing PyInstaller for building executable...
pip install pyinstaller

echo.
echo ========================================
echo Step 4: Verifying Installation
echo ========================================

echo Testing imports...

python -c "import flask; print('✓ Flask:', flask.__version__)" 2>nul || echo "❌ Flask import failed"
python -c "import flask_socketio; print('✓ Flask-SocketIO')" 2>nul || echo "❌ Flask-SocketIO import failed"
python -c "import sqlalchemy; print('✓ SQLAlchemy:', sqlalchemy.__version__)" 2>nul || echo "❌ SQLAlchemy import failed"
python -c "import pymodbus; print('✓ PyModbus:', pymodbus.__version__)" 2>nul || echo "❌ PyModbus import failed"

echo.
echo ========================================
echo Step 5: Creating Desktop Shortcuts
echo ========================================

:: Create shortcut scripts in root directory
echo Creating quick launch scripts...

:: Create quick start script
(
echo @echo off
echo cd /d "%~dp0"
echo call start_all_services.bat
) > "🚀 Start ModbusMonitor.bat"

:: Create quick stop script  
(
echo @echo off
echo cd /d "%~dp0"
echo call stop_all_services.bat
) > "🛑 Stop ModbusMonitor.bat"

:: Create setup script
(
echo @echo off
echo cd /d "%~dp0"
echo call setup_venv.bat
) > "⚙️ Setup Environment.bat"

echo ✓ Desktop shortcuts created

echo.
echo ========================================
echo Setup Complete! 🎉
echo ========================================
echo.
echo Virtual environment: %cd%\.venv
echo.
echo Quick actions:
echo • 🚀 Start ModbusMonitor.bat - Start all services
echo • 🛑 Stop ModbusMonitor.bat - Stop all services  
echo • ⚙️ Setup Environment.bat - Reinstall dependencies
echo.
echo Manual commands:
echo • start_all_services.bat - Start all services
echo • stop_all_services.bat - Stop all services
echo • setup_venv.bat - Setup virtual environment
echo.
echo Web interface will be available at:
echo http://localhost:5000
echo.
echo Press any key to start all services now...
pause >nul

echo.
echo Starting all services...
call start_all_services.bat