"""v2r(autotool.db)와 원격 PC(blogdash.db)에서 쓰는 네이버 아이디를 '우리' 업체로 등록.
감시 카페 전부에서 회원을 찾아 company_member(resolved) + 기존 글 소급.
다른 업체로 이미 등록된 아이디(예: 에코프라임)는 덮어쓰지 않는다.

    python scripts/register_our_accounts.py
"""
import json
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import cafe_api                    # noqa: E402
from src.db import Database                 # noqa: E402
from src.session import SessionManager      # noqa: E402

V2R_DB = r"C:\Users\USER\v2r\autotool.db"
REMOTE_DB = r"C:\Users\USER\RDP 자동화\blogdash\data\blogdash.db"
COMPANY = "우리"


def our_ids() -> set:
    v = sqlite3.connect(V2R_DB)
    ids = {r[0] for r in v.execute(
        "SELECT login_id FROM naver_accounts WHERE COALESCE(gone_from_v2r,0)=0 AND login_id!=''")}
    b = sqlite3.connect(REMOTE_DB)
    ids |= {r[0] for r in b.execute(
        "SELECT naver_id FROM account_cafes UNION SELECT naver_id FROM account_scan") if r[0]}
    return {i.strip().lower() for i in ids if i and i.strip()}


def main():
    cfg = json.load(open(ROOT / "config" / "settings.json", encoding="utf-8"))
    cafes = [c["cafe_id"] for c in cfg.get("cafes", [])]
    sm = SessionManager(ROOT / "data" / "sessions")
    client = cafe_api.make_client(sm.load_cookies(cfg.get("account_id")))
    if cafe_api.studio_auth_status(client) != "VERIFIED":
        print("카페 스튜디오 본인인증 필요 — 중단"); return
    db = Database(ROOT / "data" / "gamsi.db")
    db.conn.execute("PRAGMA busy_timeout=30000")

    have = dict(db.conn.execute("SELECT naver_id, company_name FROM company_master"))
    done = {(r[0], r[1]) for r in db.conn.execute(
        "SELECT naver_id, cafe_id FROM company_member WHERE company_name=? AND status IN ('resolved','not_member')",
        (COMPANY,))}
    idx = (db.conn.execute("SELECT COALESCE(MAX(row_index),900000) FROM company_master").fetchone()[0] or 900000) + 1
    ids = sorted(our_ids())
    skipped = [(i, have[i]) for i in ids if i in have and have[i] != COMPANY]
    targets = [i for i in ids if not (i in have and have[i] != COMPANY)]
    print(f"대상 {len(targets)}개 · 다른 업체라 보존 {skipped}", flush=True)

    resolved = notm = 0
    for nid in targets:
        if nid not in have:
            db.upsert_company_master(nid, COMPANY, idx); idx += 1
        for cid in cafes:
            if (nid, cid) in done:
                continue
            try:
                m = cafe_api.find_member_by_id(cid, nid, client)
            except cafe_api.StudioAuthRequired:
                print("본인인증이 풀렸습니다 — 중단"); return
            except Exception as e:
                print("  ! 검색오류", nid, cid, e); continue
            if m and m.member_key:
                db.save_member_resolution(nid, cid, m, COMPANY, "resolved")
                resolved += 1
                print(f"  ✓ {nid}@{cid} -> {m.nickname}", flush=True)
            else:
                db.save_member_resolution(nid, cid, None, COMPANY, "not_member")
                notm += 1
            time.sleep(0.3)
    n_art = db.conn.execute("SELECT COUNT(*) FROM articles WHERE company_name=?", (COMPANY,)).fetchone()[0]
    print(f"완료: 새로 찾음 {resolved} · 미가입 {notm} · '{COMPANY}' 표시된 글 {n_art}건")


if __name__ == "__main__":
    main()
