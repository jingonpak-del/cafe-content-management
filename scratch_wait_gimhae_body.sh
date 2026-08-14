# 김해 본문(상세) 보강(backfill_details) 프로세스가 끝날 때까지 대기
while :; do
  n=$(powershell -NoProfile -Command "@(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { \$_.CommandLine -match 'backfill_details' }).Count" 2>/dev/null | tr -d '[:space:]')
  if [ "$n" = "0" ] || [ -z "$n" ]; then break; fi
  sleep 600
done
echo "GIMHAE_BODY_DONE"
