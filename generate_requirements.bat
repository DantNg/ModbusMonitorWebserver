@echo off
title ModbusMonitor - Generate Requirements
echo ========================================
echo Generate Requirements.txt
echo ========================================
echo.

:: Check if virtual environment exists
if not exist .venv\Scripts\activate.bat (
    echo ❌ Virtual environment not found
    echo Please run setup_venv.bat first
    pause
    exit /b 1
)

:: Activate virtual environment
echo Activating virtual environment...
call .venv\Scripts\activate.bat

:: Check if requirements.txt already exists
if exist requirements.txt (
    echo.
    echo ⚠️  requirements.txt already exists
    echo.
    choice /c YN /m "Do you want to overwrite it? (Y/N)"
    if errorlevel 2 goto :skip_generate
)

echo.
echo Generating requirements.txt from installed packages...

:: Generate requirements.txt
pip freeze > requirements.txt

if exist requirements.txt (
    echo ✓ requirements.txt generated successfully
    echo.
    echo Contents:
    echo ----------------------------------------
    type requirements.txt
    echo ----------------------------------------
) else (
    echo ❌ Failed to generate requirements.txt
)

:skip_generate
echo.
echo ========================================
echo Requirements Generation Complete
echo ========================================

pause