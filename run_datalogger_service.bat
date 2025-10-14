@echo off
echo Starting Modbus Monitor with Datalogger Service
echo ===============================================

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Install required packages if needed
echo Checking dependencies...
python -c "import flask, sqlalchemy, requests" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    pip install flask sqlalchemy pymysql requests
)

REM Start the integrated server
echo Starting server...
python run_integrated_server.py --host 0.0.0.0 --port 5000

pause