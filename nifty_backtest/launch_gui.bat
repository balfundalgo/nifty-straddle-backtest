@echo off
title Balfund NIFTY Straddle Backtest
cd /d "%~dp0"
echo.
echo  ================================================================
echo   BALFUND -- NIFTY Straddle Backtest v2.0
echo  ================================================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found. Please install Python 3.9+
    pause & exit /b
)

:: Install deps if needed
echo  Checking dependencies...
pip install customtkinter --quiet
pip install pandas numpy openpyxl --quiet

echo  Starting GUI...
echo.
python gui.py
if errorlevel 1 (
    echo.
    echo  ERROR: GUI failed to start. See above for details.
    pause
)
