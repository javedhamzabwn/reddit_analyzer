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
  echo [ERROR] Python not found. Install Python 3.10+ and re-run.
  pause
  exit /b 1
)

echo [1/4] Creating build virtual environment...
%PYTHON_EXE% -m venv .venv-build
if errorlevel 1 (
  echo [ERROR] Failed to create .venv-build.
  pause
  exit /b 1
)
if not exist ".venv-build\Scripts\python.exe" (
  echo [ERROR] Virtual environment was not created correctly.
  pause
  exit /b 1
)

echo [2/4] Installing runtime + build dependencies...
call ".venv-build\Scripts\activate.bat"
python -m pip install --upgrade pip
set "PIP_OK=0"
for /l %%i in (1,1,3) do (
  echo [pip] Install attempt %%i/3...
  python -m pip install --retries 5 -r requirements.txt -r requirements-build.txt
  if not errorlevel 1 (
    set "PIP_OK=1"
    goto :pip_done
  )
  echo [pip] Attempt %%i failed.
)
:pip_done
if not "%PIP_OK%"=="1" (
  echo [ERROR] Dependency installation failed after 3 attempts.
  pause
  exit /b 1
)

echo [3/4] Building one-file executable...
python -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --name "RedditResearchWorkspace-OneFile" ^
  --onefile ^
  --collect-all streamlit ^
  --collect-all requests ^
  --add-data "app.py;." ^
  --add-data "reddit_status_checker.py;." ^
  --add-data "daily_scan_presets.json;." ^
  launcher.py
if errorlevel 1 (
  echo [ERROR] PyInstaller build failed.
  pause
  exit /b 1
)

echo [4/4] Done.
echo EXE path: %cd%\dist\RedditResearchWorkspace-OneFile.exe
echo Share this single EXE file with users.
pause
endlocal
