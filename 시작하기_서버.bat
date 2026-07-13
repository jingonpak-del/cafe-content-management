@echo off
chcp 65001 >nul
title 카페 게시글 감시 서버 (이 창을 닫지 마세요)
cd /d "%~dp0"

:restart
echo ============================================================
echo   Cafe Gamsi Server  -  http://localhost:8010
echo   Keep this window OPEN. Auto-restart if it stops.
echo ============================================================

rem 8010 포트를 이미 쓰는 이전 서버가 있으면 종료
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8010" ^| findstr LISTENING') do (
  taskkill /F /PID %%p >nul 2>&1
)

python -m src.server --port 8010
echo.
echo [!] Server stopped. Restarting in 5 seconds...
timeout /t 5 >nul
goto restart
