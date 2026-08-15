"""장소정보 추출 1단계 — 구조화(템플릿/주소) 기반 고정밀 추출.

카페 맛집글은 작성 템플릿(지역:/업체명:/업체위치:)과 네이버 플레이스 주소
임베드를 가진 경우가 많다. 우선 이 반정형 데이터를 정규식으로 추출해
places.db 의 place_mention 에 적재한다. (자유서술·댓글의 LLM 추출은 다음 단계)

확장성: category 컬럼으로 맛집/술집/세탁소 등 모든 카테고리를 같은 스키마에 수용.
"""
import sqlite3, re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data" / "gamsi.db"
DST = ROOT / "data" / "places.db"

CAFE_NAME = {14793916: "마산맘", 21031223: "김해줌마"}

# 경남권 주소 (도로명/지번)
ADDR = re.compile(r"(경상남도|경남)\s*([가-힣]+시)\s*([가-힣]+구)?\s*([가-힣0-9]+(?:로|길|동|읍|면))\s*([0-9][0-9\-]*)?")
# 템플릿 필드
F_NAME = re.compile(r"업체\s*명\s*[:：]\s*([^\n\r]{1,40})")
F_REGION = re.compile(r"지역\s*[:：]\s*([^\n\r]{1,30})")
# 제목 패턴: "<지역> 맛집 <업체명>"  또는 "<지역> <업체명> 맛집"
T_MATJIP = re.compile(r"(.+?)\s*맛집\s*(.+)")


def schema(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS place_mention (
        id INTEGER PRIMARY KEY,
        cafe_id INT, article_id INT, source TEXT,     -- body|comment
        category TEXT,                                 -- 맛집 (확장: 술집/세탁소…)
        raw_name TEXT,                                 -- 추출 상호
        region TEXT,                                   -- 지역 필드(동네 힌트)
        sigungu TEXT, address TEXT,
        title TEXT, write_ts INT, cur_read INT, cur_comment INT,
        method TEXT,                                   -- template|address|title
        snippet TEXT,
        UNIQUE(cafe_id, article_id, raw_name)
    );
    CREATE INDEX IF NOT EXISTS ix_pm_name ON place_mention(raw_name);
    CREATE INDEX IF NOT EXISTS ix_pm_sig ON place_mention(sigungu);
    CREATE INDEX IF NOT EXISTS ix_pm_cat ON place_mention(category);
    """)
    db.commit()


def clean(s):
    return re.sub(r"\s+", " ", (s or "").strip()).strip(" .·~,:")


def extract_one(title, body):
    """반정형 필드에서 (raw_name, region, sigungu, address, method) 추출. 없으면 None."""
    body = body or ""; title = title or ""
    name = region = sigungu = address = None
    method = None
    m = ADDR.search(body)
    if m:
        sigungu = clean((m.group(2) or "") + (" " + m.group(3) if m.group(3) else ""))
        address = clean(m.group(0))
    fn = F_NAME.search(body)
    if fn:
        name = clean(fn.group(1)); method = "template"
    fr = F_REGION.search(body)
    if fr:
        region = clean(fr.group(1))
    if not name:
        # 제목에서 업체명 추정: "김해 내동 맛집 사량도횟집" → 맛집 뒤쪽
        tm = T_MATJIP.search(title)
        if tm:
            left, right = clean(tm.group(1)), clean(tm.group(2))
            # 오른쪽이 상호일 확률 높음(맛집 뒤). 비면 왼쪽 끝 단어.
            name = right or left.split()[-1] if left else None
            region = region or left
            method = method or "title"
    if not name:
        return None
    if not method:
        method = "address" if address else "title"
    return name, region, sigungu, address, method


def main():
    src = sqlite3.connect(str(SRC)); src.row_factory = sqlite3.Row
    db = sqlite3.connect(str(DST))
    schema(db)
    # 후보: 맛집 관련 + (템플릿필드 or 주소 or 제목에 맛집)
    q = """SELECT cafe_id, article_id, title, content_text, write_ts, cur_read, cur_comment
           FROM articles
           WHERE content_text IS NOT NULL AND length(content_text)>30
             AND ( content_text LIKE '%업체명 :%' OR content_text LIKE '%경상남도 %'
                   OR content_text LIKE '%경남 %' OR title LIKE '%맛집%' )"""
    n = ins = 0
    for r in src.execute(q):
        n += 1
        ex = extract_one(r["title"], r["content_text"])
        if not ex:
            continue
        name, region, sigungu, address, method = ex
        if not name or len(name) < 2 or len(name) > 40:
            continue
        snip = clean(r["content_text"])[:180]
        try:
            db.execute(
                """INSERT OR IGNORE INTO place_mention
                   (cafe_id,article_id,source,category,raw_name,region,sigungu,address,
                    title,write_ts,cur_read,cur_comment,method,snippet)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (r["cafe_id"], r["article_id"], "body", "맛집", name, region, sigungu,
                 address, r["title"], r["write_ts"], r["cur_read"], r["cur_comment"], method, snip))
            ins += db.total_changes and 1 or 0
        except Exception:
            pass
    db.commit()
    total = db.execute("SELECT COUNT(*) FROM place_mention").fetchone()[0]
    print(f"후보 {n:,}건 스캔 → place_mention {total:,}건 적재")


if __name__ == "__main__":
    main()
