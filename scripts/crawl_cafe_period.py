"""감시 대상이 아닌 카페의 특정 기간 게시글을 미리 크롤 (서포터즈 활동분석용).

예) python scripts/crawl_cafe_period.py ladyview 2026-09-01 2026-09-30 --comment-from 2026-08-25

- 전체글 목록을 최신부터 넘기며 [comment_from, end] 사이 글을 articles 에 저장(재방문 예약 없음)
- 각 글 본문(글자수·사진수)과 댓글(작성자 member_key 포함)을 수집
- 글 통계는 start~end 작성글만 잡히고, 댓글 통계는 기간 내 작성 댓글이 잡힌다.
  기간 초에 달린 댓글이 전달 말 글에 있을 수 있어 comment_from 부터 글을 함께 수집한다.
- 이미 본문을 받은 글은 건너뛰므로 중단 후 다시 실행하면 이어서 진행.
"""
import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import cafe_api                    # noqa: E402
from src.db import Database                 # noqa: E402
from src.session import SessionManager      # noqa: E402

GAP = 0.6


def ms(d: str, end=False) -> int:
    t = dt.datetime.strptime(d, "%Y-%m-%d")
    if end:
        t += dt.timedelta(days=1)
    return int(time.mktime(t.timetuple()) * 1000) - (1 if end else 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cafe_url")
    ap.add_argument("start")
    ap.add_argument("end")
    ap.add_argument("--comment-from", default=None)
    ap.add_argument("--account", default=None)
    a = ap.parse_args()

    cfg = json.load(open(ROOT / "config" / "settings.json", encoding="utf-8"))
    acc = a.account or cfg.get("account_id")
    sm = SessionManager(ROOT / "data" / "sessions")
    if not sm.verify_live(acc).ok:
        print("로그인 세션 만료:", acc); return
    client = cafe_api.make_client(sm.load_cookies(acc))
    db = Database(ROOT / "data" / "gamsi.db")
    db.conn.execute("PRAGMA busy_timeout=30000")

    cid = cafe_api.resolve_club_id(a.cafe_url, client)
    lo = ms(a.comment_from or a.start)
    hi = ms(a.end, end=True)
    print(f"[{a.cafe_url}] club_id={cid}  수집구간 {a.comment_from or a.start} ~ {a.end}", flush=True)

    # 1) 목록 → 기간 내 글 저장
    targets, page = [], 1
    while True:
        time.sleep(GAP)
        arts = cafe_api.fetch_article_list(cid, menu_id=0, per_page=50, page=page, client=client)
        if not arts:
            break
        dated = [x for x in arts if x.write_ts and not x.is_notice]
        keep = [x for x in dated if lo <= x.write_ts <= hi and not x.blinded]
        if keep:
            db.upsert_member_articles(keep)
            targets += keep
        if dated and min(x.write_ts for x in dated) < lo:
            break
        page += 1
    print(f"목록 {page}p 확인 → 대상 글 {len(targets)}건", flush=True)

    # 2) 본문·댓글
    done = skip = gone = err = cmts = 0
    for i, x in enumerate(targets, 1):
        r = db.conn.execute("SELECT body_crawled FROM articles WHERE cafe_id=? AND article_id=?",
                            (cid, x.article_id)).fetchone()
        if r and r["body_crawled"]:
            skip += 1
            continue
        try:
            time.sleep(GAP)
            body = cafe_api.fetch_article_body(cid, x.article_id, menu_id=x.menu_id, client=client)
            db.save_body(body)
            if x.comment_count:
                time.sleep(GAP)
                cs = cafe_api.fetch_comments(cid, x.article_id, client=client)
                db.save_comments(cid, x.article_id, cs, phase="period")
                cmts += len(cs)
            done += 1
        except cafe_api.ArticleGoneError:
            db.mark_deleted(cid, x.article_id)
            gone += 1
        except Exception as e:
            err += 1
            print(f"  ! {x.article_id}: {e}", flush=True)
            time.sleep(5)
        if i % 100 == 0:
            print(f"  {i}/{len(targets)}  수집 {done} · 건너뜀 {skip} · 삭제 {gone} · 오류 {err} · 댓글 {cmts}", flush=True)
    print(f"완료: 대상 {len(targets)} · 수집 {done} · 건너뜀 {skip} · 삭제 {gone} · 오류 {err} · 댓글 {cmts}", flush=True)
    n = db.conn.execute("SELECT COUNT(*) FROM articles WHERE cafe_id=? AND write_ts BETWEEN ? AND ?",
                        (cid, ms(a.start), hi)).fetchone()[0]
    print(f"DB의 {a.start}~{a.end} 작성글: {n}건", flush=True)


if __name__ == "__main__":
    main()
