@echo off
title Test Teardown - Xoa du lieu [TEST]
cd /d "%~dp0"

echo ========================================
echo   TEST TEARDOWN - Xoa du lieu [TEST]
echo ========================================
echo.
echo Script nay se xoa:
echo   - Device [TEST] Alarm Device + tags
echo   - Subdashboard [TEST] Tag Monitor
echo   - Subdashboard [TEST] QTag Monitor
echo   - Alarm conditions + states lien quan
echo   - Notifications lien quan den [TEST]
echo.
pause

if exist .venv\Scripts\python.exe (
    .venv\Scripts\python.exe test_teardown_alarm.py
) else (
    python test_teardown_alarm.py
)

echo.
echo ========================================
echo Hoan tat! Nho khoi dong lai alarm_worker.
echo ========================================
pause
