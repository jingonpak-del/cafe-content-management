"""이미 DB에 있으나 본문/댓글이 안 채워진 글의 상세(본문+댓글)를 채운다.

backfill_recent.py 는 'DB에 없는 번호'만 새로 가져오므로, 목록만 수집돼
body_crawled=0 이거나 댓글 미수집인 기존 글은 손대지 않는다. 이 스크립트가
그런 글의 본문/댓글을 보강한다. 큐레이션(맛집 top100 등)에 본문·댓글 필요.

최근글 우선(write_ts 내림차순), 재시작 안전(진행하며 각 글 즉시 반영).
사용:  python scripts/backfill_details.py [cafe_id] [--older YEARS]
       인자 없으면 전체 카페, 최근→과거 순.
"""
import sys, time, datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.session import SessionManager
from src import cafe_api
from src.db import Database

ROOT = Path(__file__).resolve().parents[1]
THROTTLE = 0.25
LOG = ROOT / "data" / "backfill_details.log"


def log(msg):
    line = f"{datetime.datetime.now():%H:%M:%S} {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def targets(db, cafe_id):
    """본문 없음 OR (댓글이 있다는데 comments 테이블 비어있음) 인 글. 최근순."""
    return db.conn.execute(
        """SELECT a.cafe_id, a.article_id, a.menu_id, a.body_crawled, a.cur_comment, a.write_ts
           FROM articles a
           WHERE a.cafe_id=?
             AND a.status!='deleted'
             AND ( COALESCE(a.body_crawled,0)=0
                   OR ( a.cur_comment>0 AND NOT EXISTS(
                        SELECT 1 FROM comments c
                        WHERE c.cafe_id=a.cafe_id AND c.article_id=a.article_id) ) )
           ORDER BY a.write_ts DESC""",
        (cafe_id,)).fetchall()


def run_cafe(db, client, cafe_id, name):
    rows = targets(db, cafe_id)
    total = len(rows)
    log(f"[{name}] 대상 {total:,}건 (본문/댓글 보강)")
    body_done = cmt_done = gone = 0
    for i, r in enumerate(rows, 1):
        aid = r["article_id"]
        mid = r["menu_id"] or 0
        try:
            if not r["body_crawled"]:
                body = cafe_api.fetch_article_body(cafe_id, aid, menu_id=mid, client=client)
                db.save_body(body)
                body_done += 1
                time.sleep(THROTTLE)
            if r["cur_comment"] and r["cur_comment"] > 0:
                comments = cafe_api.fetch_comments(cafe_id, aid, client=client)
                if comments:
                    db.save_comments(cafe_id, aid, comments, phase="detail")
                    cmt_done += 1
                time.sleep(THROTTLE)
        except cafe_api.ArticleGoneError:
            db.mark_deleted(cafe_id, aid)
            gone += 1
            continue
        except Exception:
            continue
        if i % 200 == 0:
            d = datetime.datetime.fromtimestamp(r["write_ts"]/1000).strftime("%Y-%m-%d") if r["write_ts"] else "?"
            log(f"[{name}] {i:,}/{total:,} · 본문 {body_done:,} 댓글 {cmt_done:,} 삭제 {gone:,} (현재 {d})")
    log(f"[{name}] 완료 · 본문 {body_done:,} 댓글 {cmt_done:,} 삭제확정 {gone:,}")


def main():
    only = None
    for a in sys.argv[1:]:
        if a.isdigit():
            only = int(a)
    sm = SessionManager(ROOT / "data" / "sessions")
    import json
    cfg = json.load(open(ROOT / "config" / "settings.json", encoding="utf-8"))
    acc = cfg.get("account_id")
    if not sm.verify(acc).ok:
        log("세션 없음/만료 — 중단"); return
    client = cafe_api.make_client(sm.load_cookies(acc))
    db = Database(str(ROOT / "data" / "gamsi.db"))
    db.conn.execute("PRAGMA busy_timeout=20000")
    names = {c["cafe_id"]: c.get("name", str(c["cafe_id"])) for c in cfg.get("cafes", [])}
    cafes = [only] if only else [c["cafe_id"] for c in cfg.get("cafes", [])]
    log(f"=== 상세(본문/댓글) 보강 시작: {cafes} ===")
    for cid in cafes:
        try:
            run_cafe(db, client, cid, names.get(cid, str(cid)))
        except Exception as e:
            log(f"[{cid}] 오류: {e}")
    log("=== 완료 ===")


if __name__ == "__main__":
    main()
