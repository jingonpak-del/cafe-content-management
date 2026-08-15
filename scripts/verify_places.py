"""추출된 장소가 실제 존재/영업중인지 네이버 검색 오픈API로 1차 검증.

사전 준비: developers.naver.com 애플리케이션 등록 → "검색" API 사용 설정 →
config/settings.json 에 naver_openapi.client_id / client_secret 채우기.
(등록 5분, 무료. 지역검색은 상호명이 등록된 업체만 반환하므로 "네이버에 등록된
업체인지"는 이 API로 확인 가능. 단, 이 API 응답에는 폐업 여부 필드가 없어서
영업중/폐업 최종 판정은 2단계(플레이스 상세 페이지 확인)가 별도로 필요 — 아래
verify_one() 의 note 를 참고.

사용:  python scripts/verify_places.py [--limit N]
"""
import sys, time, json, sqlite3
from pathlib import Path
import httpx

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "places.db"


def load_keys():
    cfg = json.load(open(ROOT / "config" / "settings.json", encoding="utf-8"))
    k = cfg.get("naver_openapi") or {}
    cid, sec = k.get("client_id"), k.get("client_secret")
    if not cid or not sec:
        print("config/settings.json 에 naver_openapi.client_id/client_secret 을 먼저 채워주세요.")
        print("발급: https://developers.naver.com/apps/#/register (검색 API 체크)")
        sys.exit(1)
    return cid, sec


def search_local(client, cid, sec, query):
    r = client.get(
        "https://openapi.naver.com/v1/search/local.json",
        params={"query": query, "display": 3},
        headers={"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": sec},
        timeout=10,
    )
    if r.status_code != 200:
        return None
    items = r.json().get("items", [])
    return items[0] if items else None


def schema(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS place_verify (
        raw_name TEXT PRIMARY KEY,
        query TEXT, found INTEGER, api_title TEXT, api_addr TEXT,
        api_category TEXT, api_link TEXT, checked_at INTEGER
    );
    """)
    db.commit()


def main():
    limit = 200
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    cid, sec = load_keys()
    db = sqlite3.connect(str(DB))
    schema(db)
    rows = db.execute(
        """SELECT raw_name, region, sigungu, COUNT(*) mentions
           FROM place_mention
           WHERE raw_name NOT IN (SELECT raw_name FROM place_verify)
           GROUP BY raw_name ORDER BY mentions DESC LIMIT ?""", (limit,)).fetchall()
    print(f"검증 대상 {len(rows)}건")
    client = httpx.Client()
    found = 0
    for name, region, sigungu, mentions in rows:
        query = f"{sigungu or region or ''} {name}".strip()
        item = search_local(client, cid, sec, query)
        import re, time as _t
        if item:
            title = re.sub("<.*?>", "", item.get("title", ""))
            db.execute(
                """INSERT OR REPLACE INTO place_verify
                   (raw_name,query,found,api_title,api_addr,api_category,api_link,checked_at)
                   VALUES (?,?,1,?,?,?,?,?)""",
                (name, query, title, item.get("roadAddress") or item.get("address"),
                 item.get("category"), item.get("link"), int(_t.time() * 1000)))
            found += 1
        else:
            db.execute(
                """INSERT OR REPLACE INTO place_verify
                   (raw_name,query,found,checked_at) VALUES (?,?,0,?)""",
                (name, query, int(_t.time() * 1000)))
        db.commit()
        time.sleep(0.1)
    print(f"네이버에 등록 확인됨 {found}/{len(rows)}")
    print("주의: 이 API는 폐업여부를 알려주지 않습니다. 영업중/폐업 최종판정은 별도 확인 필요.")


if __name__ == "__main__":
    main()
