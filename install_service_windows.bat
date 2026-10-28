@echo off
REM Installs the SMEC Bridge as a Windows service using NSSM, so it starts
REM automatically on boot and restarts itself if it ever crashes -- the
REM Windows equivalent of the systemd service used on Linux.
REM
REM BEFORE RUNNING THIS:
REM   1. Run setup_windows.bat first.
REM   2. Download NSSM from https://nssm.cc/download, extract it, and copy
REM      nssm.exe (from the win64 folder) into THIS SAME FOLDER as this script.
REM   3. Run this script AS ADMINISTRATOR (right-click -> Run as administrator).

set SCRIPT_DIR=%~dp0
set SERVICE_NAME=SmecBridge

if not exist "%SCRIPT_DIR%nssm.exe" (
    echo nssm.exe not found in this folder.
    echo Download it from https://nssm.cc/download and place nssm.exe here first.
    pause
    exit /b 1
)

echo Installing service "%SERVICE_NAME%"...
"%SCRIPT_DIR%nssm.exe" install %SERVICE_NAME% "%SCRIPT_DIR%venv\Scripts\python.exe" "%SCRIPT_DIR%app.py"
"%SCRIPT_DIR%nssm.exe" set %SERVICE_NAME% AppDirectory "%SCRIPT_DIR%"
"%SCRIPT_DIR%nssm.exe" set %SERVICE_NAME% AppRestartDelay 5000
"%SCRIPT_DIR%nssm.exe" set %SERVICE_NAME% Start SERVICE_AUTO_START
"%SCRIPT_DIR%nssm.exe" set %SERVICE_NAME% AppStdout "%SCRIPT_DIR%service_output.log"
"%SCRIPT_DIR%nssm.exe" set %SERVICE_NAME% AppStderr "%SCRIPT_DIR%service_output.log"

echo.
echo Service installed. Starting it now...
"%SCRIPT_DIR%nssm.exe" start %SERVICE_NAME%

echo.
echo Done. The app will now start automatically every time this PC boots,
echo and restart itself automatically if it ever crashes.
echo.
echo Useful commands (run as Administrator):
echo   Stop:      nssm stop %SERVICE_NAME%
echo   Start:     nssm start %SERVICE_NAME%
echo   Uninstall: nssm remove %SERVICE_NAME% confirm
echo   Logs:      type service_output.log
echo   Or manage it visually via services.msc
pause
