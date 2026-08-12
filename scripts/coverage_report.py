"""각 카페가 어느 날짜까지 저장돼 있는지 + 연도별 밀도 요약."""
import sqlite3, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
c = sqlite3.connect(str(ROOT / "data" / "gamsi.db"))
CAFES = {14793916: "마산맘", 21031223: "김해줌마"}


def ymd(ts):
    return datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d") if ts else "-"


print("=== 최종 커버리지 ===")
for cid, name in CAFES.items():
    r = c.execute(
        "SELECT COUNT(*), MIN(write_ts), MAX(write_ts), "
        "SUM(CASE WHEN body_crawled=1 THEN 1 ELSE 0 END) FROM articles WHERE cafe_id=?",
        (cid,)).fetchone()
    print(f"[{name}] {r[0]:,}건 · {ymd(r[1])} ~ {ymd(r[2])} · 본문 {r[3]:,}")
    for yr in c.execute(
        "SELECT strftime('%Y',write_ts/1000,'unixepoch') y, COUNT(*) n "
        "FROM articles WHERE cafe_id=? GROUP BY y ORDER BY y DESC LIMIT 5", (cid,)):
        print(f"    {yr[0]}: {yr[1]:,}")
