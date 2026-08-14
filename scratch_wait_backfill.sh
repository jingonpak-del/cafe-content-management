# 백필 스크립트(backfill_recent / backfill_details)가 모두 끝날 때까지 대기
while :; do
  n=$(powershell -NoProfile -Command "@(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { \$_.CommandLine -match 'backfill_recent|backfill_details' }).Count" 2>/dev/null | tr -d '[:space:]')
  if [ "$n" = "0" ] || [ -z "$n" ]; then break; fi
  sleep 600
done
echo "ALL_BACKFILL_DONE"
