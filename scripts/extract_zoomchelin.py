"""'줌슐랭 리스트' 게시판(menu_id=1556) 전용 파서.

스탭이 회원 추천을 모아 정리한 게시판이라 신뢰도가 매우 높고, 형식도 정형적:
  ◼상호명
  ✔주소
  ✔영업시간
  ✔전화번호/휴무일
게시글 제목 자체가 보통 지역+카테고리(예: "진해 경화동 카페 리스트")라 지역/
주제 태깅도 거의 그대로 가능. LLM 없이 정규식만으로 고정밀 추출된다.
"""
import sqlite3, re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data" / "gamsi.db"
OUT = ROOT / "data" / "zoomchelin_list.md"

ENTRY = re.compile(
    r"◼\s*([^\n\r✔◼]{1,40})"          # 상호명
    r"((?:\s*✔[^\n\r◼]{0,120})*)",     # 뒤따르는 ✔ 라인들(주소/시간/전화 등)
)
ADDR_LINE = re.compile(r"(경상남도|경남)\s*[가-힣0-9 \-]{4,60}")
PHONE_LINE = re.compile(r"0\d{1,2}-?\d{3,4}-?\d{4}")
TIME_LINE = re.compile(r"\d{1,2}:\d{2}\s*~\s*\d{1,2}:\d{2}")
CLOSED_LINE = re.compile(r"휴무|정기휴무|후무")

# 게시글 제목에서 지역+주제 추출: "진해 경화동 카페 리스트 줌마렐라..." 등
TITLE_TOPIC = re.compile(r"^([가-힣]+)\s*([가-힣0-9]*)\s*(카페|맛집|갈비|백숙|횟집|국밥|양식|한식|중식|일식|술집|분식|고기|찜|국수)")


def parse_entries(text):
    out = []
    for m in ENTRY.finditer(text or ""):
        name = m.group(1).strip()
        tail = m.group(2) or ""
        if not name or len(name) < 2:
            continue
        addr = None
        am = ADDR_LINE.search(tail)
        if am:
            addr = re.sub(r"\s+", " ", am.group(0)).strip()
        phone = None
        pm = PHONE_LINE.search(tail)
        if pm:
            phone = pm.group(0)
        hours = None
        tm = TIME_LINE.search(tail)
        if tm:
            hours = tm.group(0)
        closed = bool(CLOSED_LINE.search(tail))
        out.append({"name": name, "address": addr, "phone": phone,
                     "hours": hours, "has_closed_day": closed})
    return out


def main():
    gc = sqlite3.connect(str(SRC)); gc.row_factory = sqlite3.Row
    rows = gc.execute(
        "SELECT article_id, title, content_text, write_ts, cur_read, cur_comment "
        "FROM articles WHERE menu_id=1556 ORDER BY write_ts DESC").fetchall()

    lines = ["# 줌슐랭 리스트 — 스탭 정리 회원추천 맛집 (전체)", ""]
    lines.append(f"원본 게시판: 줌슐랭 리스트 (menu_id=1556) · 총 {len(rows)}건 게시글\n")
    total_places = 0
    for r in rows:
        entries = parse_entries(r["content_text"])
        if not entries:
            continue
        total_places += len(entries)
        import datetime
        d = datetime.datetime.fromtimestamp(r["write_ts"] / 1000).strftime("%Y-%m-%d") if r["write_ts"] else "?"
        lines.append(f"## {r['title']}  <sub>({d} · 조회{r['cur_read']:,} · 댓글{r['cur_comment']})</sub>")
        lines.append("| 상호 | 주소 | 영업시간 | 전화 |")
        lines.append("|---|---|---|---|")
        for e in entries:
            lines.append(f"| {e['name']} | {e['address'] or '-'} | {e['hours'] or '-'} | {e['phone'] or '-'} |")
        lines.append("")
    lines.insert(2, f"(정리된 업체 총 {total_places}건)\n")
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"게시글 {len(rows)}건 중 파싱성공 {sum(1 for r in rows if parse_entries(r['content_text']))}건 · "
          f"업체 총 {total_places}건 → {OUT}")


if __name__ == "__main__":
    main()
