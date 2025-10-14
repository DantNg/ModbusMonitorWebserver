@echo off
title ModbusMonitor - Virtual Environment Setup
echo ========================================
echo ModbusMonitor Virtual Environment Setup
echo ========================================
echo.

:: Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python is not installed or not in PATH
    echo Please install Python 3.8+ first
    echo Download from: https://python.org/downloads/
    pause
    exit /b 1
)

:: Display Python version
echo ✓ Python found:
python --version

:: Change to project directory
cd /d "%~dp0"

:: Check if .venv already exists
if exist .venv (
    echo.
    echo ⚠️  Virtual environment already exists in .venv folder
    echo.
    choice /c YN /m "Do you want to recreate it? (Y/N)"
    if errorlevel 2 goto :skip_create
    if errorlevel 1 (
        echo Removing existing virtual environment...
        rmdir /s /q .venv
    )
)

echo.
echo ========================================
echo Creating Virtual Environment
echo ========================================

:: Create virtual environment
echo Creating virtual environment in .venv folder...
python -m venv .venv

if not exist .venv\Scripts\activate.bat (
    echo ❌ Failed to create virtual environment
    pause
    exit /b 1
)

echo ✓ Virtual environment created successfully

:skip_create
echo.
echo ========================================
echo Activating Virtual Environment
echo ========================================

:: Activate virtual environment
call .venv\Scripts\activate.bat

if "%VIRTUAL_ENV%"=="" (
    echo ❌ Failed to activate virtual environment
    pause
    exit /b 1
)

echo ✓ Virtual environment activated

echo.
echo ========================================
echo Installing Dependencies
echo ========================================

:: Upgrade pip first
echo Upgrading pip...
python -m pip install --upgrade pip

:: Check if requirements.txt exists
if exist requirements.txt (
    echo.
    echo Installing packages from requirements.txt...
    pip install -r requirements.txt
    
    if %errorlevel% neq 0 (
        echo.
        echo ⚠️  Some packages failed to install
        echo Please check the error messages above
        echo.
    ) else (
        echo ✓ All packages installed successfully
    )
) else (
    echo.
    echo ⚠️  requirements.txt not found
    echo Installing essential packages manually...
    
    echo Installing Flask and related packages...
    pip install flask flask-socketio eventlet
    
    echo Installing database packages...
    pip install sqlalchemy pymysql mysql-connector-python
    
    echo Installing Modbus packages...
    pip install pymodbus
    
    echo Installing other utilities...
    pip install python-dotenv requests schedule
    
    echo.
    echo ✓ Essential packages installed
    echo Consider creating a requirements.txt file for your project
)

echo.
echo ========================================
echo Virtual Environment Setup Complete!
echo ========================================
echo.
echo Virtual environment location: %cd%\.venv
echo.
echo To activate the virtual environment manually:
echo   .venv\Scripts\activate.bat
echo.
echo To deactivate:
echo   deactivate
echo.
echo Next steps:
echo 1. Run: start_all_services.bat (will auto-activate .venv)
echo 2. Or activate manually and run individual scripts
echo.

pause