@echo off
setlocal EnableDelayedExpansion

title Build License Tools

echo ============================================================
echo   Modbus Monitor  --  Build License Tools
echo ============================================================
echo.

:: ── Locate .venv ────────────────────────────────────────────────────────────
set "VENV_ACTIVATE=%~dp0.venv\Scripts\activate.bat"
if not exist "%VENV_ACTIVATE%" (
    echo [ERROR] .venv not found at: %VENV_ACTIVATE%
    echo         Run setup_venv.bat first.
    pause
    exit /b 1
)
call "%VENV_ACTIVATE%"
echo [OK] Virtual environment activated.
echo.

:: ── Ensure PyInstaller is available ─────────────────────────────────────────
python -m PyInstaller --version >nul 2>&1
if errorlevel 1 (
    echo [INFO] PyInstaller not found -- installing...
    pip install pyinstaller --quiet
    if errorlevel 1 (
        echo [ERROR] Failed to install PyInstaller.
        pause
        exit /b 1
    )
)
echo [OK] PyInstaller ready.
echo.

:: ── Output folder ───────────────────────────────────────────────────────────
set "OUT_DIR=%~dp0dist\license_tools"
if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

:: ── Build read_machine_uid.exe ───────────────────────────────────────────────
echo [1/2] Building read_machine_uid.exe ...
python -m PyInstaller ^
    --onefile ^
    --console ^
    --name read_machine_uid ^
    --distpath "%OUT_DIR%" ^
    --workpath "%~dp0build\license_tools" ^
    --specpath "%~dp0build\license_tools" ^
    --clean ^
    --noconfirm ^
    "%~dp0read_machine_uid.py"

if errorlevel 1 (
    echo [ERROR] Build failed for read_machine_uid.exe
    pause
    exit /b 1
)
echo [OK] read_machine_uid.exe  ->  %OUT_DIR%
echo.

:: ── Build generate_license.exe ──────────────────────────────────────────────
echo [2/2] Building generate_license.exe ...
python -m PyInstaller ^
    --onefile ^
    --console ^
    --name generate_license ^
    --distpath "%OUT_DIR%" ^
    --workpath "%~dp0build\license_tools" ^
    --specpath "%~dp0build\license_tools" ^
    --clean ^
    --noconfirm ^
    "%~dp0generate_license.py"

if errorlevel 1 (
    echo [ERROR] Build failed for generate_license.exe
    pause
    exit /b 1
)
echo [OK] generate_license.exe  ->  %OUT_DIR%
echo.

:: ── Done ────────────────────────────────────────────────────────────────────
echo ============================================================
echo   Build complete!  Output: %OUT_DIR%
echo ============================================================
echo.
echo   Distribute to clients:   read_machine_uid.exe
echo   Keep for admin use only: generate_license.exe
echo.
pause
endlocal
