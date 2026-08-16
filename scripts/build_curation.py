"""place_mention → 지역/소분류별 큐레이션 리포트 생성 (2단계: 정규화·랭킹).

같은 가게가 여러 글에서 언급된 경우 raw_name 기준으로 묶어 places 테이블에
정규화하고, 시군구별·소분류별 top 리스트를 마크다운으로 출력한다.
"""
import sqlite3, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "places.db"


def schema(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS place (
        id INTEGER PRIMARY KEY,
        category TEXT, subcategory TEXT, name TEXT,
        sigungu TEXT, region TEXT, address TEXT,
        mentions INT, total_read INT, total_comment INT,
        first_seen INT, last_seen INT, promo_flag INT DEFAULT 0,
        UNIQUE(category, name, sigungu)
    );
    """)
    db.commit()


def normalize_name(n):
    n = re.sub(r"\s+", " ", n).strip()
    n = re.sub(r"(김해점|삼계점|본점|율하점)$", "", n).strip()
    return n


def build(db):
    rows = db.execute(
        """SELECT category, subcategory, raw_name, region, sigungu, address,
                  write_ts, cur_read, cur_comment, promo_flag
           FROM place_mention""").fetchall()
    agg = {}
    for cat, subcat, raw, region, sigungu, addr, wts, read, cmt, promo in rows:
        name = normalize_name(raw)
        key = (cat, name, sigungu or "")
        d = agg.setdefault(key, {"subcats": {}, "region": None, "addr": None,
                                  "mentions": 0, "read": 0, "cmt": 0,
                                  "first": wts, "last": wts, "promo": 0})
        d["mentions"] += 1
        d["read"] += read or 0
        d["cmt"] += cmt or 0
        d["first"] = min(d["first"], wts or d["first"])
        d["last"] = max(d["last"], wts or d["last"])
        d["promo"] = max(d["promo"], promo or 0)
        if subcat:
            d["subcats"][subcat] = d["subcats"].get(subcat, 0) + 1
        if not d["region"] and region:
            d["region"] = region
        if not d["addr"] and addr:
            d["addr"] = addr
    db.execute("DELETE FROM place")
    for (cat, name, sigungu), d in agg.items():
        subcat = max(d["subcats"], key=d["subcats"].get) if d["subcats"] else None
        db.execute(
            """INSERT OR IGNORE INTO place
               (category,subcategory,name,sigungu,region,address,mentions,
                total_read,total_comment,first_seen,last_seen,promo_flag)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (cat, subcat, name, sigungu, d["region"], d["addr"], d["mentions"],
             d["read"], d["cmt"], d["first"], d["last"], d["promo"]))
    db.commit()
    return len(agg)


def report(db, min_mentions=2, top_n=15):
    lines = ["# 맛집 큐레이션 리포트 (지역×소분류, 2단계 정규화)", ""]
    sigungus = [r[0] for r in db.execute(
        "SELECT DISTINCT sigungu FROM place WHERE sigungu IS NOT NULL AND sigungu!='' "
        "ORDER BY sigungu")]
    for sg in sigungus:
        rows = db.execute(
            """SELECT name, subcategory, mentions, total_read, total_comment, address, promo_flag
               FROM place WHERE sigungu=? AND mentions>=?
               ORDER BY mentions DESC, total_read DESC LIMIT ?""",
            (sg, min_mentions, top_n)).fetchall()
        if not rows:
            continue
        lines.append(f"## {sg} (언급≥{min_mentions}회, 상위 {len(rows)})")
        lines.append("| 상호 | 종류 | 언급 | 조회 | 댓글 | 주소 |")
        lines.append("|---|---|---|---|---|---|")
        for name, subcat, m, read, cmt, addr, promo in rows:
            mark = " ⚠️광고의심" if promo else ""
            lines.append(f"| {name}{mark} | {subcat or '-'} | {m} | {read:,} | {cmt} | {addr or '-'} |")
        lines.append("")
    # 소분류별 전체 top (지역 무관, 종류 큐레이션용)
    subcats = [r[0] for r in db.execute(
        "SELECT DISTINCT subcategory FROM place WHERE subcategory IS NOT NULL ORDER BY subcategory")]
    lines.append("---\n# 종류별 전체 TOP (지역 무관)\n")
    for sc in subcats:
        rows = db.execute(
            """SELECT name, sigungu, mentions, total_read FROM place
               WHERE subcategory=? AND mentions>=? ORDER BY mentions DESC, total_read DESC LIMIT 10""",
            (sc, min_mentions)).fetchall()
        if not rows:
            continue
        lines.append(f"## {sc}")
        for name, sg, m, read in rows:
            lines.append(f"- **{name}** ({sg or '-'}) · 언급{m} · 조회{read:,}")
        lines.append("")
    return "\n".join(lines)


def main():
    db = sqlite3.connect(str(DB))
    schema(db)
    n = build(db)
    print(f"정규화된 고유 장소: {n:,}건")
    out = report(db)
    path = ROOT / "data" / "curation_report.md"
    path.write_text(out, encoding="utf-8")
    print(f"저장: {path}")


if __name__ == "__main__":
    main()
