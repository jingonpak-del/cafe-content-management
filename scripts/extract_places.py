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

# 상호가 아닌 것(질문형 문구·템플릿 라벨·감탄사·문장파편)을 걸러내는 블랙리스트
NAME_BLACKLIST = re.compile(
    r"(추천|알려주세요|있나요|있을까요|해주세요|부탁|궁금|어디로|가시나요|"
    r"업체위치|편집바|장소\s*클릭|이벤트|댓글|말고|찾아요|찾습니다|줌슐랭|리스트|"
    r"^이에요|^이예요|^소개|^다녀왔|^\?+$|^ㅎ+$|^ㅋ+$|"
    r"(어요|네요|아요|해요|왔어요|봤어요|드려요)[!.]*$)"
)
# 이모지 데코 홍보성 제목(!\s+... 또는 다중 이모지)은 광고 템플릿일 가능성 높음 — 별도 플래그
PROMO_DECO = re.compile(r"^[!.]\s|[\U0001F300-\U0001FAFF].*[\U0001F300-\U0001FAFF]")

# "초기 글"(진짜 개인 후기) 판별은 단어매칭이 아니라 작성빈도로 한다: 한 계정이
# 서로 다른 업체를 몇 곳이나 리뷰했는지 — 많으면(예:5곳+) 전문 후기/광고성 계정으로 간주.
GENUINE_MAX_DISTINCT_PLACES = 5

# 음식종류(소분류) 키워드 규칙 — 제목+본문 텍스트에서 매칭
SUBCATEGORY_RULES = [
    (r"갈비|고기집|삼겹|목살|양꼬치|숯불|구이", "고기/구이"),
    (r"횟집|물회|회덮밥|\b회\b", "횟집/해산물"),
    (r"해물찜|아구찜|물총|조개", "해물"),
    (r"국수|칼국수|냉면", "국수"),
    (r"국밥|순대국|해장국", "국밥/해장"),
    (r"추어탕", "추어탕"),
    (r"스시|초밥|사시미", "일식/스시"),
    (r"짬뽕|짜장|중화|중국집", "중식"),
    (r"돈까스|카츠", "돈까스"),
    (r"파스타|피자|스테이크|양식", "양식"),
    (r"브런치|샐러드", "브런치"),
    (r"베이커리|빵집|디저트|케이크", "베이커리/디저트"),
    (r"카페|커피|라떼", "카페"),
    (r"한정식|반상|백반|정식", "한정식/백반"),
    (r"곱창|막창|양대창", "곱창/막창"),
    (r"족발|보쌈", "족발/보쌈"),
    (r"마라탕|양꼬치", "중화요리"),
    (r"분식|떡볶이", "분식"),
    (r"치킨", "치킨"),
]


def guess_subcategory(text):
    for pat, cat in SUBCATEGORY_RULES:
        if re.search(pat, text):
            return cat
    return None


def schema(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS place_mention (
        id INTEGER PRIMARY KEY,
        cafe_id INT, article_id INT, source TEXT,     -- body|comment
        category TEXT,                                 -- 맛집 (확장: 술집/세탁소…)
        subcategory TEXT,                               -- 음식종류 등 소분류
        raw_name TEXT,                                 -- 추출 상호
        region TEXT,                                   -- 지역 필드(동네 힌트)
        sigungu TEXT, address TEXT,
        title TEXT, write_ts INT, cur_read INT, cur_comment INT,
        method TEXT,                                   -- template|address|title|comment
        snippet TEXT,
        promo_flag INT DEFAULT 0,                       -- 이모지데코 등 광고성 의심
        genuine INT DEFAULT 1,                          -- 개인서술 있음(실제후기)=1, 정보나열형=0
        author_member_key TEXT, author_level TEXT,      -- 작성자(댓글weighting과 동일 체계)
        comment_id INT DEFAULT 0,                       -- source='comment'일 때만 사용
        level_weight REAL DEFAULT 1.0,                  -- 작성자 등급 가중치(댓글 추천에 적용)
        UNIQUE(cafe_id, article_id, source, comment_id, raw_name)
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
    if NAME_BLACKLIST.search(name):
        return None
    if not method:
        method = "address" if address else "title"
    return name, region, sigungu, address, method


def main():
    src = sqlite3.connect(str(SRC)); src.row_factory = sqlite3.Row
    db = sqlite3.connect(str(DST))
    schema(db)
    # 후보: 맛집 관련 + (템플릿필드 or 주소 or 제목에 맛집)
    q = """SELECT cafe_id, article_id, title, content_text, write_ts, cur_read, cur_comment,
                  member_key, member_level, company_name
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
        subcat = guess_subcategory((r["title"] or "") + " " + (r["content_text"] or ""))
        promo = 1 if PROMO_DECO.search(name) or PROMO_DECO.search(r["title"] or "") else 0
        # 광고주 계정(등록업체/기관) 작성글은 홍보성으로 간주
        if r["company_name"] or r["member_level"] == "상생업체/기관":
            promo = 1
        try:
            db.execute(
                """INSERT OR IGNORE INTO place_mention
                   (cafe_id,article_id,source,category,subcategory,raw_name,region,sigungu,address,
                    title,write_ts,cur_read,cur_comment,method,snippet,promo_flag,genuine,
                    author_member_key,author_level)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)""",
                (r["cafe_id"], r["article_id"], "body", "맛집", subcat, name, region, sigungu,
                 address, r["title"], r["write_ts"], r["cur_read"], r["cur_comment"], method, snip, promo,
                 r["member_key"], r["member_level"]))
            ins += db.total_changes and 1 or 0
        except Exception:
            pass
    db.commit()
    # 작성빈도 기반 "초기 글" 판별: 서로 다른 업체를 GENUINE_MAX_DISTINCT_PLACES곳 넘게
    # 리뷰한 계정은 전문 후기/광고성 계정으로 보고 genuine=0 처리.
    heavy = db.execute(
        """SELECT author_member_key FROM place_mention
           WHERE source='body' AND author_member_key IS NOT NULL AND author_member_key!=''
           GROUP BY author_member_key
           HAVING COUNT(DISTINCT raw_name) > ?""", (GENUINE_MAX_DISTINCT_PLACES,)).fetchall()
    heavy_keys = [h[0] for h in heavy]
    for mk in heavy_keys:
        db.execute("UPDATE place_mention SET genuine=0 WHERE author_member_key=?", (mk,))
    db.commit()
    print(f"전문후기/광고성 계정(서로다른업체>{GENUINE_MAX_DISTINCT_PLACES}) {len(heavy_keys)}명 → 해당 게시글 genuine=0 처리")
    total = db.execute("SELECT COUNT(*) FROM place_mention").fetchone()[0]
    print(f"후보 {n:,}건 스캔 → place_mention {total:,}건 적재")


if __name__ == "__main__":
    main()
