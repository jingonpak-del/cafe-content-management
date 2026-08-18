"""스탭(매일이벤트)이 회원들에게 지역맛집을 물어본 질문글 + 댓글 추천 정리.

"🔍OO 맛집 알려주세요!! 💬댓글 이벤트💬" 패턴의 스탭 질문글에 달린 댓글에서
이미 추출·LLM검증된 place_mention(source='comment')을 질문글 단위로 묶어
보기 좋게 정리한다(extract_comment_recs.py 의 결과를 재사용).
"""
import sqlite3, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GAMSI = ROOT / "data" / "gamsi.db"
PLACES = ROOT / "data" / "places.db"
OUT = ROOT / "data" / "staff_qna_report.md"

LEVEL_ORDER = {"여신렐라": 5, "부 매니저": 5, "여왕렐라": 4, "전체 게시판 스탭": 4,
               "개별 게시판 스탭": 4, "공주렐라": 3, "명예렐라": 3, "이벤트 스탭": 3,
               "소녀렐라": 2}


def main():
    gc = sqlite3.connect(str(GAMSI)); gc.row_factory = sqlite3.Row
    pc = sqlite3.connect(str(PLACES)); pc.row_factory = sqlite3.Row

    articles = gc.execute(
        """SELECT article_id, title, write_ts, cur_read, cur_comment
           FROM articles WHERE title LIKE '🔍%' AND title LIKE '%맛집%'
           ORDER BY write_ts DESC"""
    ).fetchall()

    lines = ["# 스탭 질문글 — 회원 댓글 지역맛집 추천 정리", ""]
    lines.append(
        "'매일이벤트' 게시판에서 스탭이 올린 지역맛집 질문글에 회원들이 댓글로 "
        "달아준 업체명을 모았다. 댓글은 LLM으로 검증했고, 작성자 카페등급을 "
        "함께 표기(등급 높을수록 신뢰도 가중치 ↑).\n"
    )

    total_q = total_recs = 0
    for a in articles:
        recs = pc.execute(
            """SELECT raw_name, subcategory, author_level, snippet FROM place_mention
               WHERE source='comment' AND category IS NOT NULL
                 AND article_id=? AND title=?
               ORDER BY 1""",
            (a["article_id"], a["title"])).fetchall()
        if not recs:
            continue
        total_q += 1
        total_recs += len(recs)
        d = datetime.datetime.fromtimestamp(a["write_ts"] / 1000).strftime("%Y-%m-%d") if a["write_ts"] else "?"
        title_clean = a["title"].replace("🔍", "").replace("💬댓글 이벤트💬", "").strip()
        lines.append(f"## {title_clean}  <sub>({d} · 댓글{a['cur_comment']})</sub>")
        # 등급 높은 순 정렬
        recs_sorted = sorted(recs, key=lambda r: -(LEVEL_ORDER.get(r["author_level"], 1)))
        lines.append("| 업체명 | 종류 | 추천자 등급 | 댓글 원문 |")
        lines.append("|---|---|---|---|")
        for r in recs_sorted:
            lv = r["author_level"] or "미상"
            snip = (r["snippet"] or "").replace("|", "/")[:60]
            lines.append(f"| {r['raw_name']} | {r['subcategory'] or '-'} | {lv} | {snip} |")
        lines.append("")

    lines.insert(2, f"(질문글 {total_q}건 · 추천 {total_recs}건)\n")
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"질문글 {total_q}건, 추천 {total_recs}건 → {OUT}")


if __name__ == "__main__":
    main()
