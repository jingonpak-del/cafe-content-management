"""백필 감독: 각 카페가 기준일(2024-01-01)에 도달할 때까지 backfill_recent 를
살려둔다. 프로세스가 외부 요인으로 죽으면 자동 재기동(resume). 두 카페가 모두
기준일에 도달하면 완료 마커(data/ALL_BACKFILL_DONE) 생성.

진행 판정: data/backfill_recent.log 의 마지막 '[카페] ... 현재글 YYYY-MM-DD'.
nohup 으로 실행 → 세션/에이전트 종료에도 생존.
"""
import re, time, subprocess, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "data" / "backfill_recent.log"
SUPLOG = ROOT / "data" / "supervisor.log"
MARKER = ROOT / "data" / "ALL_BACKFILL_DONE"
FLOOR = "2024-01-01"
CAFES = {14793916: "마산맘", 21031223: "김해줌마"}


def slog(m):
    with open(SUPLOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.datetime.now():%F %T} {m}\n")


def running(cafe_id):
    ps = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         "@(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
         f"Where-Object {{ $_.CommandLine -match 'backfill_recent.*--cafe {cafe_id}' }}).Count"],
        capture_output=True, text=True)
    return ps.stdout.strip() not in ("", "0")


def last_date(cafe_name):
    if not LOG.exists():
        return None
    d = None
    pat = re.compile(rf"\[{cafe_name}\].*현재글 (\d{{4}}-\d{{2}}-\d{{2}})")
    for line in LOG.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = pat.search(line)
        if m:
            d = m.group(1)
    return d


def launch(cafe_id):
    out = open(ROOT / "data" / f"backfill_recent_{cafe_id}.out", "a", encoding="utf-8")
    subprocess.Popen(
        ["python", "-u", "scripts/backfill_recent.py", "--cafe", str(cafe_id), "--floor", FLOOR],
        cwd=str(ROOT), stdout=out, stderr=out)
    slog(f"[{CAFES[cafe_id]}] 재기동")


def main():
    if MARKER.exists():
        MARKER.unlink()
    slog(f"감독 시작 (floor {FLOOR})")
    while True:
        done = {}
        for cid, name in CAFES.items():
            ld = last_date(name)
            reached = bool(ld and ld <= FLOOR)
            if reached:
                done[cid] = True
            elif not running(cid):
                launch(cid)
                done[cid] = False
                time.sleep(20)
            else:
                done[cid] = False
        if all(done.get(c) for c in CAFES):
            slog("두 카페 모두 기준일 도달 → 완료")
            subprocess.run(["python", "scripts/coverage_report.py"], cwd=str(ROOT))
            MARKER.write_text("done\n")
            slog("=== 마커 생성 완료 ===")
            break
        time.sleep(300)


if __name__ == "__main__":
    main()
