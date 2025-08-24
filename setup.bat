@echo off
:: Step 1: Create virtual environment
echo Creating virtual environment...
python3 -m venv .venv

:: Step 2: Activate virtual environment
echo Activating virtual environment...
call .venv\Scripts\activate

:: Step 3: Install required libraries
echo Installing required libraries...
pip install --upgrade pip
pip install -r requirements.txt



