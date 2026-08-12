"""최근 N년치 과거글을 '번호 역순'으로 채운다.

전체글 목록 API는 최근 ~5만건만 주므로, 그 이전 기간은 게시글 번호를 직접
위(최신)→아래(과거)로 훑어야 한다. 이 스크립트는 각 카페의 최대 번호부터
내려가며, 이미 DB에 있는 번호는 건너뛰고(HTTP 없음), 없는 번호만 본문을
가져와 저장한다. 작성일이 기준일(N년 전)보다 오래되면 그 카페는 종료.

재시작 안전: 진행 위치를 meta(recent_bf_{cid})에 저장, 다음 실행 때 이어서.
사용:  python scripts/backfill_recent.py [years]   (기본 2년)
"""
import sys, time, datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.session import SessionManager
from src import cafe_api
from src.db import Database

ROOT = Path(__file__).resolve().parents[1]
THROTTLE = 0.22          # 초/요청 (~4.5 req/s)
BELOW_FLOOR_STOP = 40    # 기준일보다 오래된 글이 연속 N개면 종료
LOG = ROOT / "data" / "backfill_recent.log"


def log(msg):
    line = f"{datetime.datetime.now():%H:%M:%S} {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def recent_block_bottom(db, cafe_id, gap=400):
    """최신에서부터 촘촘히 존재하는(=목록수집 완료) 구간의 하단 번호를 찾는다.
    번호 내림차순으로 훑다가 gap 이상 연속 빈 번호가 나오면 그 지점이 실제 누락 시작.
    → 그 위(촘촘한 구간)는 이미 커버됐으므로 fetch 낭비를 피한다."""
    ids = [r[0] for r in db.conn.execute(
        "SELECT article_id FROM articles WHERE cafe_id=? ORDER BY article_id DESC", (cafe_id,))]
    if not ids:
        return 0
    prev = ids[0]
    for a in ids[1:]:
        if prev - a > gap:
            return prev      # 여기서부터 아래로 누락 시작
        prev = a
    return prev              # 전부 촘촘 → 가장 오래된 것


def backfill_cafe(db, client, cafe_id, name, floor_ms):
    row = db.conn.execute("SELECT MAX(article_id), MIN(article_id) FROM articles WHERE cafe_id=?",
                          (cafe_id,)).fetchone()
    top = row[0] or 0
    resume = db.get_meta(f"recent_bf_{cafe_id}")
    # 최신(top)부터 내려가며 누락분을 fetch. 이미 있는 번호는 HTTP 없이 스킵되므로
    # 촘촘한 최근 구간은 빠르게 지나가고, 실제 누락(옛 구간/중간 구멍)만 채운다.
    start = int(resume) if resume else top
    log(f"[{name}] 시작 num={start:,} (top={top:,}) → 기준일 {datetime.datetime.fromtimestamp(floor_ms/1000):%Y-%m-%d}")
    num = start
    got = fetched = skipped = gone = 0
    below = 0
    while num > 1:
        num -= 1
        # 이미 DB에 있으면 그 글의 write_ts로 기준일 판단 후 스킵 (HTTP 없음)
        r = db.conn.execute("SELECT write_ts FROM articles WHERE cafe_id=? AND article_id=?",
                            (cafe_id, num)).fetchone()
        if r:
            skipped += 1
            if r[0] and r[0] < floor_ms:
                below += 1
                if below >= BELOW_FLOOR_STOP:
                    break
            else:
                below = 0
            continue
        # 없으면 본문 fetch
        try:
            body = cafe_api.fetch_article_body(cafe_id, num, menu_id=0, client=client)
        except cafe_api.ArticleGoneError:
            gone += 1
            time.sleep(THROTTLE)
            if fetched and (fetched + gone) % 500 == 0:
                db.set_meta(f"recent_bf_{cafe_id}", str(num))
            continue
        except Exception as e:
            time.sleep(THROTTLE)
            continue
        fetched += 1
        wt = body.write_ts or 0
        if wt and wt < floor_ms:
            below += 1
            if below >= BELOW_FLOOR_STOP:
                break
        else:
            below = 0
            if db.upsert_body_as_article(cafe_id, num, body):
                got += 1
        if fetched % 200 == 0:
            db.set_meta(f"recent_bf_{cafe_id}", str(num))
            d = datetime.datetime.fromtimestamp(wt/1000).strftime("%Y-%m-%d") if wt else "?"
            log(f"[{name}] num={num:,} 저장 {got:,} (fetch {fetched:,}/삭제 {gone:,}/스킵 {skipped:,}) 현재글 {d}")
        time.sleep(THROTTLE)
    db.set_meta(f"recent_bf_{cafe_id}", str(num))
    log(f"[{name}] 완료 num={num:,} · 신규저장 {got:,} (fetch {fetched:,}, 삭제 {gone:,}, 스킵 {skipped:,})")


def main():
    """인자:
      [years]            최근 N년 (기본 2). 위치 인자(숫자).
      --floor YYYY-MM-DD 명시적 기준일(이보다 오래되면 종료). years보다 우선.
      --cafe CAFE_ID     특정 카페만 (여러 번 지정 가능). 없으면 전체.
    예) python backfill_recent.py --cafe 21031223 --floor 2024-01-01
    """
    args = sys.argv[1:]
    years = 2.0
    floor_date = None
    only_cafes = []
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--floor":
            floor_date = args[i + 1]; i += 2; continue
        if a == "--cafe":
            only_cafes.append(int(args[i + 1])); i += 2; continue
        if a.replace(".", "").isdigit():
            years = float(a)
        i += 1
    if floor_date:
        floor_ms = int(datetime.datetime.strptime(floor_date, "%Y-%m-%d").timestamp() * 1000)
    else:
        floor_ms = int((time.time() - years * 365 * 86400) * 1000)
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
    cafes = only_cafes or [c["cafe_id"] for c in cfg.get("cafes", [])]
    log(f"=== 과거글 백필 시작 (floor {datetime.datetime.fromtimestamp(floor_ms/1000):%Y-%m-%d}, 카페 {cafes}) ===")
    for cid in cafes:
        try:
            backfill_cafe(db, client, cid, names.get(cid, str(cid)), floor_ms)
        except Exception as e:
            log(f"[{cid}] 오류: {e}")
    log("=== 전체 완료 ===")


if __name__ == "__main__":
    main()
