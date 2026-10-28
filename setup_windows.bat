@echo off
REM Run this once, from inside the smec_bridge folder.
REM Requires Python 3.11+ already installed with "Add to PATH" checked.

echo Creating virtual environment...
python -m venv venv
if errorlevel 1 (
    echo FAILED - is Python installed and on PATH? Try "python --version" to check.
    pause
    exit /b 1
)

echo Installing dependencies...
venv\Scripts\pip install -r requirements.txt

echo.
echo Done. To run the app manually and check it works:
echo     venv\Scripts\python app.py
echo.
echo Then open http://localhost:8000 in a browser on this PC.
echo Once that works, run install_service.bat to make it run automatically.
pause
