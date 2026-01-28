@echo off
setlocal
cd /d "%~dp0.."

call ".venv\Scripts\activate.bat"

if not exist "logs" mkdir logs

python 01_ingestion\polymarket_snapshot_once.py >> logs\snapshot.log 2>&1

endlocal

