"""방치된 not_member 회원해석을 재시도한다 (세션 유효 시 대량 복구).
resolve 성공분은 save_member_resolution 이 기존 게시글에 소급 매칭까지 수행한다."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.session import SessionManager
from src import cafe_api
from src.db import Database


def main():
    root = Path(__file__).resolve().parents[1]
    sm = SessionManager(root / "data" / "sessions")
    # 설정에서 account_id 로드
    import json
    cfg = json.load(open(root / "config" / "settings.json", encoding="utf-8"))
    acc = cfg.get("account_id")
    if not sm.verify(acc).ok:
        print("세션 없음/만료 — 재로그인 필요:", acc)
        return
    client = cafe_api.make_client(sm.load_cookies(acc))
    db = Database(str(root / "data" / "gamsi.db"))

    rows = db.conn.execute(
        "SELECT naver_id, cafe_id, company_name FROM company_member WHERE status='not_member' ORDER BY resolved_at"
    ).fetchall()
    total = len(rows)
    print(f"대상 not_member: {total}건")
    fixed = 0
    for i, r in enumerate(rows, 1):
        nid, cid, company = r["naver_id"], r["cafe_id"], r["company_name"]
        try:
            m = cafe_api.find_member_by_id(cid, nid, client)
            if m and m.member_key:
                db.save_member_resolution(nid, cid, m, company, "resolved")
                fixed += 1
                print(f"  [{i}/{total}] ✓ {nid}@{cid} -> {m.nickname} ({company})")
            # 못 찾으면 not_member 유지 (resolved_at 갱신해 뒤로 미룸)
            else:
                db.conn.execute(
                    "UPDATE company_member SET resolved_at=? WHERE naver_id=? AND cafe_id=?",
                    (int(time.time() * 1000), nid, cid),
                )
                db.conn.commit()
        except Exception as e:
            print(f"  [{i}/{total}] ! {nid}@{cid}: {e}")
        time.sleep(0.35)
    print(f"완료: {fixed}건 복구 / {total}건 중")


if __name__ == "__main__":
    main()
