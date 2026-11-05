"""기존에 달린 광고 링크 댓글 일괄 처리: 차단 도메인/.info 댓글 계정 정지 + 그 댓글 삭제.

    python scripts/linkspam_backfill.py            # 목록만
    python scripts/linkspam_backfill.py --execute
"""
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import cafe_api, link_guard as L        # noqa: E402
from src.db import Database                      # noqa: E402
from src.session import SessionManager           # noqa: E402


class _W:
    def log(self, m): print(m, flush=True)


def main():
    db = Database(ROOT / "data" / "gamsi.db")
    db.conn.execute("PRAGMA busy_timeout=30000")
    L.ensure_tables(db)
    auto, _ = L.load_block(db)     # 자동 차단 도메인만 (나머지는 화면에서 확인 후 승인)
    g = L.LinkGuard(_W())
    hits = defaultdict(lambda: {"nick": "", "domains": set()})
    for cid, mk, nk, ct in db.conn.execute(
            "SELECT cafe_id, member_key, writer_nickname, content FROM comments "
            "WHERE cafe_id IN (14793916, 21031223) AND is_deleted=0 AND content LIKE '%.%'"):
        k = L.classify(ct, auto, set())
        d = k and k[0] == "auto" and k[1]
        if d and mk and not g.exempt(db, cid, mk):
            hits[(cid, mk)]["nick"] = nk
            hits[(cid, mk)]["domains"].add(d)
    print(f"대상 {len(hits)}개 계정", flush=True)
    if "--execute" not in sys.argv:
        return
    client = cafe_api.make_client(SessionManager(ROOT / "data" / "sessions").load_cookies("snowgreent"))
    for (cid, mk), h in hits.items():
        g.punish(db, client, cid, mk, h["nick"], h["domains"],
                lambda t: any(L._match(x, auto) for x in L.domains_in(t)))
    print("완료", flush=True)


if __name__ == "__main__":
    main()
