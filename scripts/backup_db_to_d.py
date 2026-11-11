"""gamsi.db 를 D드라이브로 안전하게 백업(sqlite 온라인 백업 → 서버를 멈추지 않아도 일관된 사본).
최근 KEEP 개만 남기고 오래된 것은 지운다.

    python scripts/backup_db_to_d.py
"""
import sqlite3
import time
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "data" / "gamsi.db"
DST_DIR = Path(r"D:\claude-archive\gamsi-backup")
KEEP = 7


def main():
    DST_DIR.mkdir(parents=True, exist_ok=True)
    dst = DST_DIR / f"gamsi-{time.strftime('%Y%m%d-%H%M')}.db"
    t = time.time()
    src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True, timeout=60)
    out = sqlite3.connect(dst)
    src.backup(out, pages=2000, sleep=0.05)   # 조각씩 복사해 서버 쓰기를 막지 않는다
    ok = out.execute("PRAGMA quick_check").fetchone()[0]
    n = out.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    out.close(); src.close()
    print(f"{dst} {dst.stat().st_size/1048576:.0f}MB · 점검 {ok} · 글 {n}건 · {time.time()-t:.0f}초")
    if ok != "ok":
        dst.unlink(); raise SystemExit("점검 실패 — 사본 삭제")
    for old in sorted(DST_DIR.glob("gamsi-*.db"))[:-KEEP]:
        old.unlink(); print("삭제", old.name)


if __name__ == "__main__":
    main()
