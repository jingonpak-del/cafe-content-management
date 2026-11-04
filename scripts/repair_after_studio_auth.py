"""카페 스튜디오 본인인증 후 밀린 회원조회 복구 (2026-10-28 회원관리 이전 대응).

1) 본인인증 상태 확인 (VERIFIED 아니면 중단)
2) 10/28 이후 '미가입'으로 기록된 업체 아이디 재조회 → 업체 매칭·게시글 소급
3) 10/28 이후 멤버태그를 못 받은 게시글 작성자 태그 채우기 + 업체 매칭

    python scripts/repair_after_studio_auth.py
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import cafe_api                    # noqa: E402
from src.db import Database                 # noqa: E402
from src.session import SessionManager      # noqa: E402

CUT = int(time.mktime(time.strptime("2026-10-28", "%Y-%m-%d")) * 1000)


def main():
    cfg = json.load(open(ROOT / "config" / "settings.json", encoding="utf-8"))
    acc = cfg.get("account_id")
    sm = SessionManager(ROOT / "data" / "sessions")
    client = cafe_api.make_client(sm.load_cookies(acc))
    st = cafe_api.studio_auth_status(client)
    print("카페 스튜디오 본인인증:", st, flush=True)
    if st != "VERIFIED":
        print("→ 본인인증 후 다시 실행하세요."); return
    db = Database(ROOT / "data" / "gamsi.db")
    db.conn.execute("PRAGMA busy_timeout=30000")

    # 2) 업체 아이디 재조회
    rows = db.conn.execute(
        "SELECT naver_id, cafe_id, company_name FROM company_member "
        "WHERE status='not_member' AND resolved_at>=? AND naver_id NOT LIKE 'tag:%'", (CUT,)).fetchall()
    print(f"업체 아이디 재조회 대상 {len(rows)}건", flush=True)
    fixed = 0
    for i, r in enumerate(rows, 1):
        try:
            m = cafe_api.find_member_by_id(r["cafe_id"], r["naver_id"], client)
        except cafe_api.StudioAuthRequired:
            print("본인인증이 풀렸습니다 — 중단"); return
        except Exception as e:
            print("  !", r["naver_id"], e); continue
        if m and m.member_key:
            db.save_member_resolution(r["naver_id"], r["cafe_id"], m, r["company_name"], "resolved")
            fixed += 1
        else:
            db.conn.execute("UPDATE company_member SET resolved_at=? WHERE naver_id=? AND cafe_id=?",
                            (int(time.time() * 1000), r["naver_id"], r["cafe_id"]))
            db.conn.commit()
        if i % 200 == 0:
            print(f"  {i}/{len(rows)} · 복구 {fixed}", flush=True)
        time.sleep(0.3)
    print(f"업체 아이디 복구 {fixed}건", flush=True)

    # 3) 멤버태그 빠진 작성자
    who = db.conn.execute(
        """SELECT cafe_id, member_key, writer_nickname FROM articles
           WHERE (member_tag IS NULL OR member_tag='') AND member_key!='' AND first_seen_at>=?
           GROUP BY cafe_id, member_key""", (CUT,)).fetchall()
    print(f"멤버태그 채울 작성자 {len(who)}명", flush=True)
    tagged = 0
    for w in who:
        try:
            m = cafe_api.resolve_member_by_key(w["cafe_id"], w["member_key"], w["writer_nickname"], client)
        except cafe_api.StudioAuthRequired:
            print("본인인증이 풀렸습니다 — 중단"); return
        if m:
            db.cache_member(w["cafe_id"], w["member_key"], m.member_tag, m.nickname,
                            m.masked_id, m.level_name, resolved=True)
            info = db.get_company_by_member_key(w["cafe_id"], w["member_key"]) or {}
            db.conn.execute(
                """UPDATE articles SET member_tag=?, member_level=?,
                       company_name=COALESCE(?, company_name)
                   WHERE cafe_id=? AND member_key=? AND (member_tag IS NULL OR member_tag='')""",
                (m.member_tag, m.level_name, info.get("company_name"), w["cafe_id"], w["member_key"]))
            db.conn.commit()
            tagged += 1
        time.sleep(0.3)
    print(f"멤버태그 채움 {tagged}/{len(who)}명 · 완료", flush=True)


if __name__ == "__main__":
    main()
