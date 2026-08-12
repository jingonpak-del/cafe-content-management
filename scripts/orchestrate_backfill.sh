#!/bin/bash
# 백필 연쇄 오케스트레이터 (nohup 으로 실행 → 세션 종료에도 생존).
#  1) 김해 본문보강(backfill_details) 끝나면 → 김해 2024~ 누락 게시글 수집 시작
#  2) 마산맘/김해 backfill_recent 가 모두 끝나면 → 완료 마커 + 최종 커버리지 기록
cd /c/Users/USER/게시글감시 || exit 1
LOG=data/orchestrate.log

running() {  # $1 = cmdline 정규식
  n=$(powershell -NoProfile -Command "@(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { \$_.CommandLine -match '$1' }).Count" 2>/dev/null | tr -d '[:space:]')
  [ -n "$n" ] && [ "$n" != "0" ]
}

echo "$(date '+%F %T') 오케스트레이터 시작" >> "$LOG"

# 1) 김해 본문보강 완료 대기 → 김해 누락 게시글(2024-01~) 수집 시작
while running "backfill_details"; do sleep 300; done
echo "$(date '+%F %T') 김해 본문보강 완료 → 김해 2024 누락수집 시작" >> "$LOG"
nohup python -u scripts/backfill_recent.py --cafe 21031223 --floor 2024-01-01 >> data/backfill_recent_gimhae.out 2>&1 &
sleep 15

# 2) 마산맘+김해 backfill_recent 모두 완료 대기
while running "backfill_recent"; do sleep 300; done
echo "$(date '+%F %T') 모든 backfill_recent 완료" >> "$LOG"

# 최종 커버리지 기록
python scripts/coverage_report.py >> "$LOG" 2>&1
touch data/ALL_BACKFILL_DONE
echo "$(date '+%F %T') === 전체 완료 (마커 생성) ===" >> "$LOG"
