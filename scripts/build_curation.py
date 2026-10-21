"""place_mention → 지역/소분류별 큐레이션 리포트 생성 (2단계: 정규화·점수랭킹).

같은 가게가 여러 글/댓글에서 언급된 경우 raw_name 기준으로 묶어 places 테이블에
정규화하고, 시군구별·소분류별 top 리스트를 마크다운으로 출력한다.

점수 산정 원칙(사장님 요청 반영):
- 본문(body) 언급: 기본 가중치 1.0. 단 "초기 글"(한 계정이 소수 업체만 리뷰)
  위주로 잡고, 서로 다른 업체를 대량으로 리뷰한 전문 후기/광고성 계정
  (genuine=0)의 글은 점수에서 완전히 제외한다.
- 댓글(comment) 추천: 정보나열형 홍보글보다 신뢰도가 높다고 보고 기본 가중치를
  BODY 대비 높게(COMMENT_BASE_WEIGHT) 준다. 추가로 댓글 작성자의 카페 등급에
  따른 level_weight를 곱해, 등급이 높을수록(여신렐라>여왕렐라>공주/명예렐라>
  소녀렐라) 그 추천에 더 큰 가중치를 준다. 등록 광고주(상생업체/기관) 계정의
  댓글은 애초에 추출 단계에서 제외됨.
- 이모지데코 등 광고의심(promo_flag) 게시글은 완전히 제외.
"""
import sqlite3, re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "places.db"

BODY_WEIGHT = 1.0
# 댓글 추천 raw level_weight 는 extract_comment_recs.py 에서 이미
# LEVEL_WEIGHT(등급) * COMMENT_BASE_WEIGHT 로 계산돼 place_mention.level_weight 에 저장됨.


def schema(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS place (
        id INTEGER PRIMARY KEY,
        category TEXT, subcategory TEXT, name TEXT,
        sigungu TEXT, region TEXT, address TEXT,
        mentions INT, body_mentions INT, comment_recs INT,
        total_read INT, total_comment INT, score REAL,
        first_seen INT, last_seen INT, promo_flag INT DEFAULT 0,
        UNIQUE(category, name, sigungu)
    );
    """)
    db.commit()


def normalize_name(n):
    n = re.sub(r"\s+", " ", n).strip()
    n = re.sub(r"(김해점|삼계점|본점|율하점)$", "", n).strip()
    return n


# 카페 소재지 — 본문에 주소가 없을 때 마지막 수단으로 쓴다(2026-10-21).
# 맘카페 회원 추천은 대부분 그 동네 가게라, 지역 미상으로 버리는 것보다 낫다.
CAFE_SIGUNGU = {21031223: "김해시", 14793916: "창원시"}
JUNK_NAME = re.compile(r"(^[^가-힣a-zA-Z]*$|^.$|입니다$|^이요$|아시는\s*분|잘\s*봤|^오호|쓰릅|^\W|\?|~!|^(근처|에서|이요|고기집|북면)$)")


def build(db):
    # genuine=0(전문후기/광고성 계정) body 언급은 애초에 제외하고 집계
    rows = db.execute(
        """SELECT category, subcategory, raw_name, region, sigungu, address,
                  write_ts, cur_read, cur_comment, promo_flag, source, level_weight, cafe_id
           FROM place_mention
           WHERE category IS NOT NULL AND promo_flag=0
             AND (source='comment' OR genuine=1)""").fetchall()

    # 댓글 추천은 sigungu가 없다(댓글 텍스트만으로는 지역 특정 불가).
    # 같은 상호가 본문에서 언급된 적 있으면 그 지역으로 귀속시킨다(가장 많이 쓰인 지역).
    name_sigungu_votes = {}
    rows = [r for r in rows if not JUNK_NAME.search(normalize_name(r[2]) or "")]
    for cat, subcat, raw, region, sigungu, addr, wts, read, cmt, promo, source, lw, _cid in rows:
        if source != "comment" and sigungu:
            name = normalize_name(raw)
            v = name_sigungu_votes.setdefault(name, {})
            v[sigungu] = v.get(sigungu, 0) + 1
    name_best_sigungu = {n: max(v, key=v.get) for n, v in name_sigungu_votes.items()}

    agg = {}
    for cat, subcat, raw, region, sigungu, addr, wts, read, cmt, promo, source, lw, cid in rows:
        name = normalize_name(raw)
        if not sigungu:
            sigungu = name_best_sigungu.get(name) or CAFE_SIGUNGU.get(cid)
        key = (cat, name, sigungu or "")
        d = agg.setdefault(key, {"subcats": {}, "region": None, "addr": None,
                                  "mentions": 0, "body": 0, "crec": 0,
                                  "read": 0, "cmt": 0, "score": 0.0,
                                  "first": wts, "last": wts})
        d["mentions"] += 1
        d["read"] += read or 0
        d["cmt"] += cmt or 0
        if wts:
            d["first"] = wts if d["first"] is None else min(d["first"], wts)
            d["last"] = wts if d["last"] is None else max(d["last"], wts)
        if source == "comment":
            d["crec"] += 1
            d["score"] += lw or 1.0     # 등급가중치 반영된 값(추출 단계에서 계산됨)
        else:
            d["body"] += 1
            d["score"] += BODY_WEIGHT
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
                body_mentions,comment_recs,total_read,total_comment,score,
                first_seen,last_seen,promo_flag)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
            (cat, subcat, name, sigungu, d["region"], d["addr"], d["mentions"],
             d["body"], d["crec"], d["read"], d["cmt"], round(d["score"], 2),
             d["first"], d["last"]))
    db.commit()
    return len(agg)


def report(db, min_score=1.5, top_n=15):
    lines = ["# 맛집 큐레이션 리포트 (지역×소분류, 점수 기반 랭킹)", ""]
    lines.append(
        "점수 = 본문언급×1.0 + 댓글추천×(작성자 카페등급 가중치). "
        "댓글 추천이 홍보성 본문보다 신뢰도 높다고 보아 더 높은 가중치를 줌. "
        "서로 다른 업체를 대량 리뷰한 전문후기/광고성 계정과 이모지데코 홍보글은 "
        "점수 계산에서 아예 제외.\n"
    )
    sigungus = [r[0] for r in db.execute(
        "SELECT DISTINCT sigungu FROM place WHERE sigungu IS NOT NULL AND sigungu!='' "
        "ORDER BY sigungu")]
    for sg in sigungus:
        rows = db.execute(
            """SELECT name, subcategory, score, body_mentions, comment_recs,
                      total_read, total_comment, address
               FROM place WHERE sigungu=? AND score>=?
               ORDER BY score DESC, total_read DESC LIMIT ?""",
            (sg, min_score, top_n)).fetchall()
        if not rows:
            continue
        lines.append(f"## {sg} (점수≥{min_score}, 상위 {len(rows)})")
        lines.append("| 상호 | 종류 | 점수 | 본문 | 댓글추천 | 조회 | 주소 |")
        lines.append("|---|---|---|---|---|---|---|")
        for name, subcat, score, body, crec, read, cmt, addr in rows:
            lines.append(f"| {name} | {subcat or '-'} | {score:.1f} | {body} | {crec} | {read:,} | {addr or '-'} |")
        lines.append("")
    # 소분류별 전체 top (지역 무관, 종류 큐레이션용)
    subcats = [r[0] for r in db.execute(
        "SELECT DISTINCT subcategory FROM place WHERE subcategory IS NOT NULL ORDER BY subcategory")]
    lines.append("---\n# 종류별 전체 TOP (지역 무관)\n")
    for sc in subcats:
        rows = db.execute(
            """SELECT name, sigungu, score, body_mentions, comment_recs FROM place
               WHERE subcategory=? AND score>=? ORDER BY score DESC LIMIT 10""",
            (sc, min_score)).fetchall()
        if not rows:
            continue
        lines.append(f"## {sc}")
        for name, sg, score, body, crec in rows:
            lines.append(f"- **{name}** ({sg or '-'}) · 점수{score:.1f} (본문{body}·댓글추천{crec})")
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
