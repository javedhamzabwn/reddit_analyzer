@echo off
setlocal
cd /d "%~dp0"

set "PYTHON_EXE="
where py >nul 2>nul
if %errorlevel%==0 (
  set "PYTHON_EXE=py -3"
) else (
  where python >nul 2>nul
  if %errorlevel%==0 (
    set "PYTHON_EXE=python"
  )
)

if "%PYTHON_EXE%"=="" (
  echo [ERROR] Python not found. Install Python 3.10+ and try again.
  pause
  exit /b 1
)

echo [1/2] Installing or updating requirements...
%PYTHON_EXE% -m pip install -r requirements.txt
if errorlevel 1 (
  echo [ERROR] Failed to install dependencies.
  pause
  exit /b 1
)

echo [2/2] Starting Streamlit app...
%PYTHON_EXE% -m streamlit run app.py
if errorlevel 1 (
  echo [ERROR] Streamlit exited with an error.
  pause
  exit /b 1
)

pause
