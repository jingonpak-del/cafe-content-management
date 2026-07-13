@echo off
chcp 65001 >nul
echo 카페 게시글 감시 서버를 종료합니다 (포트 8010)...
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8010" ^| findstr LISTENING') do (
  taskkill /F /PID %%p >nul 2>&1
  echo   서버 프로세스 종료: PID %%p
)
rem 자동재시작 bat(cmd 창)도 종료
taskkill /F /FI "WINDOWTITLE eq 카페 게시글 감시 서버*" >nul 2>&1
echo 완료. 이 창은 아무 키나 누르면 닫힙니다.
pause >nul
