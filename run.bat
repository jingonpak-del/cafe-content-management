@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo 카페 게시글 감시 시스템 시작 중...
echo http://localhost:8010 으로 접속하세요
python -m src.server --port 8010 %*
pause
