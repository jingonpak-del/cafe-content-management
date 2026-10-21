"""장소정보 추출 2단계 — 댓글 기반 추천 추출 (질문글에 달린 실제 추천 댓글).

"OO 맛집 아시나요/추천해주세요" 같은 질문글에 댓글로 달리는 추천이 광고성
정보글보다 신뢰도가 높다고 보고, 본문 추출보다 높은 기본 가중치를 준다.
추가로 댓글 작성자의 카페 등급(소녀렐라~여신렐라)에 따라 가중치를 더 준다.
등록 광고주(상생업체/기관) 계정의 댓글은 추천으로 인정하지 않는다(제외).
"""
import sqlite3, re, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_places import schema, guess_subcategory, clean, NAME_BLACKLIST  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data" / "gamsi.db"
DST = ROOT / "data" / "places.db"

# 등급 가중치 — 활동량 기반 카페 등급 위계(소녀 < 공주/명예 < 여왕 < 여신), 스탭은 신뢰도 높게.
# 상생업체/기관(등록광고주)은 0으로 완전 배제 — 추천이 아니라 자기 홍보이므로.
LEVEL_WEIGHT = {
    "소녀렐라": 1.0,
    "공주렐라": 1.3,
    "명예렐라": 1.3,
    "여왕렐라": 1.6,
    "여신렐라": 2.0,
    "전체 게시판 스탭": 1.8,
    "개별 게시판 스탭": 1.8,
    "부 매니저": 2.0,
    "이벤트 스탭": 1.5,
    "상생업체/기관": 0.0,
}
DEFAULT_WEIGHT = 1.0
COMMENT_BASE_WEIGHT = 2.0  # 댓글 추천 기본 가중치(본문 언급=1.0 대비)

# 질문글(추천 요청) 판별 — 맛집류 키워드 + 요청형 표현
Q_TOPIC = re.compile(r"맛집|밥집|카페|술집|고기집|횟집|맛있는")
Q_ASK = re.compile(r"추천|어디|아시나요|아세요|있나요|있을까요|알려|궁금|찾습니다|찾아요")

# 댓글에서 흔한 반응/동의/질문/잡담/인사 표현(상호가 아님) — 후보에서 제외
COMMENT_NOISE = re.compile(
    r"^(네|넵|넹|응|어|저도|동감|같은생각|좋아요|감사|고마워|축하|화이팅|굿|good|ㅇㅇ|"
    r"ㅋ+|ㅎ+|맞아요|그러게요|저요|나도|저기요)$|"
    r"\?|까요|나요|시나요|실까요|반갑|부럽|배불|고고|저두|저도|이사갑니까|배달은|"
    r"싶당|싶어요|싶네요|하시든|드시든|하실까|해볼게|할께요|겠어요|겠네요|"
    r"감사합니다|감사해요|고맙습니다|안녕하세요|안녕하십니까|정보\s*감사|이쁘네요|예쁘네요|"
    r"최고예요|최고에요|^최고$|배고파|배고픈|맛있어보|맛있겠|맛나욤|맛나요$|잘\s*보고|"
    r"좋은\s*정보|유용한\s*정보|도움\s*됩니다|미션\s*완료|당첨"
)
# 댓글 끝에 흔히 붙는 추천/감상 표현 — 잘라내고 앞부분을 상호 후보로
# 2026-10-21: 과거글 백필로 댓글이 3배 늘면서 인사·반응 댓글이 상호로 대량 유입
# ("정보 얻어갑니다" 점수 432 등). 반응·인사 어휘와 구어체 어미로 끝나는 댓글은 상호가 아니다.
REACTION = re.compile(
    r"(정보\s*얻|얻어\s*갑|얻어가|감사|고마|맛나|맛있|참고|저장|좋네|좋아|부럽|가봐야|가볼게|"
    r"반가|우와|와우|그쵸|그죠|꼴깍|^정보|정보\s*줍|맛집\s*정보|맛잇|^여기$|^\W*\d+\W*$|^\W+$|"
    r"요[\s!.~^ㅎㅋㅠ♡]*$|먹고\s*싶|대박|땡기|당기네|침\s*고|군침|추천\s*감|알아\s*갑|기억|메모|굿굿|"
    r"(니다|니당|어요|아요|에요|예요|네요|게요|래요|해요|죠|용|당|예|네|ㅎ|ㅋ|ㅠ|\^)[\s!.~^ㅎㅋㅠ♡]*$)")

TRAIL = re.compile(
    r"(추천드려요|추천드립니다|추천해요|추천합니다|추천이요|추천!|강추요|강추입니다|강추!*|"
    r"가보세요|가보셔요|드셔보세요|먹어보세요|괜찮아요|좋아요|좋더라구요|맛있어요|맛있더라구요|"
    r"맛있습니다|맛집이에요|맛집이예요|괜찮더라구요|한번\s*가보세요|입니다\.?|이에요\.?|예요\.?|"
    r"어때요\??|어떠세요\??|여기요|여기입니다|여기\s*가보세요)+\s*[!.~♡❤️😍👍]*$"
)


def load_level_map(gc):
    """member_key → 가장 흔한 등급명 (해당 회원이 쓴 글들에서)."""
    rows = gc.execute(
        """SELECT member_key, member_level, COUNT(*) n FROM articles
           WHERE member_level IS NOT NULL AND member_key IS NOT NULL AND member_key!=''
           GROUP BY member_key, member_level"""
    ).fetchall()
    best = {}
    for mk, lv, n in rows:
        cur = best.get(mk)
        if not cur or n > cur[1]:
            best[mk] = (lv, n)
    return {mk: lv for mk, (lv, _n) in best.items()}


def extract_name_from_comment(text):
    t = clean(text)
    if not t or len(t) < 2:
        return None
    if "?" in t or COMMENT_NOISE.search(t):
        return None
    t2 = TRAIL.sub("", t).strip(" .·~!,:")
    if not t2 or len(t2) < 2 or len(t2) > 25:
        return None
    if NAME_BLACKLIST.search(t2) or COMMENT_NOISE.search(t2) or REACTION.search(t2):
        return None
    # 한글/영문/숫자가 최소 하나는 있어야 함(순수 기호/이모지 제거)
    if not re.search(r"[가-힣a-zA-Z0-9]", t2):
        return None
    return t2


def main():
    gc = sqlite3.connect(str(SRC)); gc.row_factory = sqlite3.Row
    db = sqlite3.connect(str(DST))
    schema(db)
    level_map = load_level_map(gc)
    print(f"등급 매핑된 회원: {len(level_map):,}")

    q_articles = """SELECT cafe_id, article_id, title FROM articles
                     WHERE title IS NOT NULL"""
    q_ids = []
    for r in gc.execute(q_articles):
        if Q_TOPIC.search(r["title"]) and Q_ASK.search(r["title"]):
            q_ids.append((r["cafe_id"], r["article_id"], r["title"]))
    print(f"질문글(추천요청) 후보: {len(q_ids):,}")

    n = ins = excluded_ad = 0
    for cafe_id, article_id, title in q_ids:
        comments = gc.execute(
            """SELECT comment_id, content, member_key FROM comments
               WHERE cafe_id=? AND article_id=? AND (is_deleted IS NULL OR is_deleted=0)""",
            (cafe_id, article_id)).fetchall()
        for c in comments:
            n += 1
            name = extract_name_from_comment(c["content"])
            if not name:
                continue
            level = level_map.get(c["member_key"])
            weight = LEVEL_WEIGHT.get(level, DEFAULT_WEIGHT)
            if level == "상생업체/기관":
                excluded_ad += 1
                continue
            subcat = guess_subcategory((title or "") + " " + (c["content"] or ""))
            snip = clean(c["content"])[:180]
            try:
                db.execute(
                    """INSERT OR IGNORE INTO place_mention
                       (cafe_id,article_id,source,category,subcategory,raw_name,
                        title,method,snippet,promo_flag,genuine,author_member_key,author_level,
                        comment_id,level_weight,cur_read,cur_comment)
                       VALUES (?,?,?,?,?,?,?,?,?,0,1,?,?,?,?,0,0)""",
                    (cafe_id, article_id, "comment", "맛집", subcat, name, title, "comment",
                     snip, c["member_key"], level, c["comment_id"], weight * COMMENT_BASE_WEIGHT))
                ins += db.total_changes and 1 or 0
            except Exception:
                pass
    db.commit()
    total = db.execute("SELECT COUNT(*) FROM place_mention WHERE source='comment'").fetchone()[0]
    print(f"댓글 {n:,}건 스캔 → 추천 {total:,}건 적재 (광고주 댓글 제외 {excluded_ad})")


if __name__ == "__main__":
    main()
