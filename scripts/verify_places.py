"""큐레이션된 장소(place)를 네이버 지역검색으로 확인해 실제 상호·주소·업종을 붙인다.

키: NAVER API HUB(네이버 클라우드) — 정본 파일 naver_api.txt(1줄 ID:시크릿).
값은 출력·저장하지 않는다. 결과는 place_verify(이름키 기준)에 쌓이고,
build_curation.py 가 다음 실행 때 주소로 지역을 바로잡는 데 쓴다.
이 API에는 폐업 여부가 없다 — '네이버에 등록된 업체인지'까지만 확인.

사용:  python scripts/verify_places.py [--limit N]   (점수 높은 순, 미검증만)
"""
import re
import sys
import time
import sqlite3
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_curation import name_key  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "places.db"
KEY_FILE = Path(r"C:\Users\USER\서브프로그램\뷰티헬스사이트\pipeline\config\naver_api.txt")
HUB = "https://naverapihub.apigw.ntruss.com/search/v1/local"


def load_keys():
    if not KEY_FILE.exists():
        sys.exit(f"키 파일 없음: {KEY_FILE}")
    cid, sec = KEY_FILE.read_text(encoding="utf-8").splitlines()[0].strip().split(":", 1)
    return {"X-NCP-APIGW-API-KEY-ID": cid, "X-NCP-APIGW-API-KEY": sec}


def search(client, headers, query):
    r = client.get(HUB, params={"query": query, "display": 5}, headers=headers, timeout=10)
    if r.status_code != 200:
        return []
    return r.json().get("items", [])


# place 는 전부 '맛집' 분류 — 같은 이름의 마사지숍·대리점 등을 정답으로 잡지 않게
FOOD = re.compile(r"(음식|카페|디저트|베이커리|제과|한식|양식|중식|일식|분식|술집|주점|요리|국밥|국수|"
                  r"고기|육류|치킨|피자|뷔페|브런치|횟집|해산물|샤브|돈가스|돈까스|떡|빵|밀면|냉면|족발|키즈카페)")


def pick(items, name):
    """이름키가 서로 포함되는 김해·창원(두 카페 생활권) 업체만 인정(동명이점·엉뚱한 결과 배제)."""
    k = name_key(name)
    for it in items:
        title = re.sub("<.*?>", "", it.get("title", ""))
        addr = it.get("roadAddress") or it.get("address") or ""
        tk = name_key(title)
        if re.search(r"경상남도 (김해시|창원시)", addr) and FOOD.search(it.get("category", "")) and (k in tk or tk in k):
            return title, addr, it.get("category", ""), it.get("link", "")
    return None


def schema(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS place_verify2 (
        name_key TEXT PRIMARY KEY, name TEXT, query TEXT, found INTEGER,
        api_title TEXT, api_addr TEXT, api_category TEXT, api_link TEXT, checked_at INTEGER
    );""")


def main():
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 300
    headers = load_keys()
    db = sqlite3.connect(str(DB))
    schema(db)
    done = {r[0] for r in db.execute("SELECT name_key FROM place_verify2")}
    rows = [r for r in db.execute(
        "SELECT name, sigungu FROM place WHERE promo_flag=0 ORDER BY score DESC")
        if name_key(r[0]) not in done][:limit]
    print(f"검증 대상 {len(rows)}건")
    client = httpx.Client()
    found = 0
    for name, sigungu in rows:
        city = (sigungu or "").split(" ")[0].replace("시", "")
        hit, q = None, ""
        # 카페 지역 먼저, 없으면 다른 도시(김해 회원이 창원 가게를 추천하는 일이 잦다)
        for q in [f"{c} {name}" for c in dict.fromkeys([city, "창원", "김해"]) if c]:
            hit = pick(search(client, headers, q), name)
            if hit:
                break
            time.sleep(0.05)
        db.execute("INSERT OR REPLACE INTO place_verify2 VALUES (?,?,?,?,?,?,?,?,?)",
                   (name_key(name), name, q, 1 if hit else 0, *(hit or (None,) * 4),
                    int(time.time() * 1000)))
        found += bool(hit)
        db.commit()
    print(f"네이버 등록 확인 {found}/{len(rows)}")


if __name__ == "__main__":
    main()
