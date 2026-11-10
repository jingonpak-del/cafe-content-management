"""등업 게시판 주식 리딩방 사기글 차단 (2026-11-05).

등업 게시판에 '매니저님 덕분에 수익' 류 후기를 쓰고, 바람잡이 계정들이 댓글로
단축 링크(m.site.naver.com 등)를 돌리는 수법. 네이버가 몇 분 뒤 지우지만 그 사이 피해가 난다.

- 대상 게시판만 30초마다 목록을 보고, 최근 글은 댓글까지 다시 읽어 점수를 매긴다
- 기준을 넘으면 글 삭제(게시판 스탭 권한) + 작성자·바람잡이 댓글 계정 활동정지(카페 스튜디오)
- 처리 내역은 scam_action 에 남긴다(정지 해제는 화면 버튼, 글 복구는 카페 관리 > 삭제글)
"""
from __future__ import annotations

import json
import re
import threading
import sqlite3
import time
from pathlib import Path

import httpx

from . import cafe_api
from .db import Database

# 창원(마산맘)·김해 줌마렐라 등업 게시판
GUARD_BOARDS = [
    (14793916, 1688, "마산맘"),
    (21031223, 1106, "김해줌마"),
]
INTERVAL_S = 5               # 등업게시판 목록 확인 간격 (11-09 30초→5초)
LINK_INTERVAL_S = 30         # 광고 링크 댓글(전체 게시판)은 30초 유지
BACKOFF_S = 60               # 목록 조회가 연달아 실패하면 잠시 늦춘다(차단 방지)
RECHECK_MIN = 90            # 글 쓴 뒤 이 시간 동안은 매번 댓글을 다시 읽는다

SHORT_LINK_RE = re.compile(
    r"(m\.site\.naver\.com|naver\.me|bit\.ly|han\.gl|me2\.kr|vo\.la|url\.kr|tinyurl\.com|"
    r"buly\.kr|zrr\.kr|open\.kakao\.com|t\.me/|telegram\.me|linktr\.ee|band\.us)", re.I)
STOCK_WORDS = [
    "주식", "종목", "국장", "미장", "성투", "수익률", "수익", "추가매수", "매수", "매도", "관련주",
    "상한가", "급등", "리딩", "정보채널", "채널주소", "주식인생", "변동성", "코스피", "코스닥",
    "테마주", "단타", "스윙", "추천주", "반도체", "증시", "시드",
    # 손실 하소연형('와이프 몰래 몰빵했다가…' → 바람잡이가 리딩방 링크)
    "손절", "익절", "물타", "평단", "레버리지", "몰빵", "패닉셀", "영끌", "우량주", "불장",
    "하이닉스", "삼성전자", "리더님", "고수분들",
    # 운영진 사칭 공지형('외국인 수급… 오후장 주목할 업종', 본문은 배너 이미지)
    "수급", "업종", "오후장", "장중", "장전", "투자자", "물량", "외국인 매도",
]
BAIT_WORDS = ["매니저님", "공지", "덕분에", "소름", "터닝포인트", "꿈에도"]
INVISIBLE_RE = re.compile("[⁠​‌‍﻿ㅤᅟ]")
WARN_RE = re.compile(r"광고|사기|신고|스팸|조심|낚시|지랄")
_DOTTED_RE = re.compile(r"(?<=[가-힣])[.·ㆍ*_-](?=[가-힣])")   # 관.점.포.인.트 리.더


def _norm(t: str) -> str:
    """숨은 문자 제거 + 글자 사이 점찍기 우회 풀기."""
    return _DOTTED_RE.sub("", INVISIBLE_RE.sub("", t))


# 본문이 이미지뿐이라 주식어가 없는 유형(11-09 실측: 자삭·삭제된 글에서 놓친 것들) — 제목으로 잡는다
#  · 운영진 사칭 공지: "📌카페 커뮤니티 개설안내 줌마렐라 매니저 님이 함께", "📢<긴급공지> 스터디방 개설"
#  · 토스 이벤트 낚시: "토 스 미국 나스닥 상장 기념 1인당 5만원 현금"
IMPERSONATE_RE = re.compile(
    r"(매니[저져]|운영진|긴급\s?공지|공지\s?팀|스페셜\s?게스트|커뮤니티\s?(개설|안내|공지)|스터디\s?(그룹|방|개설)|공식\s?스터디|"
    r"폭락장\s?(대비|대응))")
TOSS_BAIT_RE = re.compile(r"토\s?스.{0,20}(5\s?만\s?원|나스닥|현금|무료\s?주식)")


def title_suspect(title: str) -> str:
    """제목만으로 보는 사기 의심 사유(없으면 빈 문자열)."""
    t = _norm(title or "")
    if IMPERSONATE_RE.search(t) and (re.search(r"^[\W_]*[📢📌✅🔥🚨💰]", t) or "줌마렐라" in t or "카페" in t):
        return "운영진 사칭 공지형 제목"
    if TOSS_BAIT_RE.search(t):
        return "토스 이벤트 낚시형 제목"
    return ""


# 11-11: 낮은 등급이 운영진 행세 닉네임을 쓰는 경우. 우리는 스탭 아이디로만 글을 올리므로 스탭이 아닌 계정이
# 닉네임에 아래 단어를 쓰고 공지·이벤트 꼴 글을 올리면 사칭으로 본다(등업글 1만9천건 검증: 걸린 44건 전부 사기글)
NICK_WORD_RE = re.compile(r"(줌마\s?렐라|공\s?지|관\s?리\s?팀|운\s?영|매니[저져])")
ANN_EMOJI_RE = re.compile(r"^[\W_]*[📢📌✅🔥🚨💰💌🎈🎁🎇🔴]")
ANN_WORD_RE = re.compile(r"(이벤트|콜라보|공지|안내|개설|스터디|필독|긴급|특별|협업|공식)")


def nick_suspect(nick: str, title: str, text_len: int, has_image: bool) -> str:
    """스탭이 아닌 글쓴이에게만 부른다. 의심 사유(없으면 빈 문자열)."""
    n = bool(NICK_WORD_RE.search(nick or ""))
    t = title or ""
    emo = bool(ANN_EMOJI_RE.search(t))
    jm = "줌마렐라" in _norm(t).replace(" ", "")
    if n and (emo or ANN_WORD_RE.search(t) or jm):
        return f"낮은 등급이 운영진 행세 닉네임('{nick}')으로 공지·이벤트형 글"
    if n and text_len < 60 and has_image:
        return f"운영진 행세 닉네임('{nick}')으로 이미지뿐인 글"
    if jm and emo:
        return "낮은 등급이 '줌마렐라' 단어를 쓴 공지형 제목"
    return ""


def score(title: str, body: str, comments: list) -> dict:
    """점수·판정 근거. hit=True 면 차단 대상."""
    raw = f"{title}\n{body}\n" + "\n".join(c.content for c in comments if not c.is_deleted)
    text = _norm(raw)
    stock = sorted({w for w in STOCK_WORDS if w in text})
    bait = sorted({w for w in BAIT_WORDS if w in f"{title}\n{body}"})
    links = sorted({m.group(1).lower() for m in SHORT_LINK_RE.finditer(text)})
    invisible = len(INVISIBLE_RE.findall(raw))
    reasons = []
    hit = False
    if links and len(stock) >= 2:
        hit = True
        reasons.append(f"단축링크({', '.join(links)}) + 주식어 {len(stock)}개")
    elif len(stock) >= 3:       # 등업글 1만8천건 실측: 정상글 중 3개 이상은 2건(둘 다 홍보성)
        hit = True
        reasons.append(f"주식어 {len(stock)}개({', '.join(stock[:6])})")
    ts = title_suspect(title)
    if ts and not hit:
        hit = True
        reasons.append(ts)
    return {"hit": hit, "stock": stock, "bait": bait, "links": links,
            "invisible": invisible, "reasons": reasons}


def bad_keys(db: Database, cafe_id: int) -> set:
    """사기글을 쓴 적 있는 계정(처리 내역·수동 정지 기록). 탈퇴했다 다시 와도 같은 계정이면 바로 잡는다."""
    ks = {r[0] for r in db.conn.execute(
        "SELECT writer_key FROM scam_action WHERE cafe_id=? AND writer_key!=''", (cafe_id,))}
    ks |= {r[0] for r in db.conn.execute("SELECT member_key FROM guard_stop WHERE cafe_id=?", (cafe_id,))}
    return ks


def accomplices(writer_key: str, comments: list) -> list[str]:
    """정지할 계정: 작성자 + 링크·주식어를 쓴 댓글 작성자(욕하는 일반회원은 제외)."""
    keys = [writer_key] if writer_key else []
    for c in comments:
        if c.is_deleted or not c.member_key or c.member_key in keys:
            continue
        t = _norm(c.content)
        if not SHORT_LINK_RE.search(t) and WARN_RE.search(t):     # "광고네요", "사기 조심" 같은 일반회원
            continue
        if SHORT_LINK_RE.search(t) or any(w in t for w in STOCK_WORDS):
            keys.append(c.member_key)
    return keys


def delete_article(client: httpx.Client, cafe_id: int, article_id: int) -> tuple[bool, str]:
    """게시판 스탭 권한 삭제. 응답은 실패해도 200 이라 재조회로 확인한다."""
    H = {"Referer": f"https://m.cafe.naver.com/ca-fe/web/cafes/{cafe_id}/articles/{article_id}",
         "Origin": "https://m.cafe.naver.com", "X-Cafe-Product": "mobile"}
    r = client.post("https://apis.naver.com/cafe-web/cafe-mobile/ArticleDelete.json",
                    data={"cafeId": cafe_id, "articleId": article_id, "requestFrom": "B"}, headers=H)
    time.sleep(1.5)
    try:
        cafe_api.fetch_article_body(cafe_id, article_id, client=client)
        return False, f"삭제 후에도 글이 보임: {r.text[:120]}"
    except cafe_api.ArticleGoneError:
        return True, "삭제 확인"


def stop_members(client: httpx.Client, cafe_id: int, member_keys: list[str]) -> tuple[int, list]:
    """카페 스튜디오 활동정지(영구, 불법광고). 해제는 release_members."""
    r = client.post(f"{cafe_api.STUDIO_API}/cafes/{cafe_id}/members/activity-stop",
                    json={"memberKeys": member_keys, "reasonType": "ILLEGAL_AD",
                          "customReasonId": None, "etcReason": None,
                          "periodDays": None, "permanent": True},
                    headers=cafe_api._studio_headers())
    if r.status_code in (401, 403):
        raise cafe_api.StudioAuthRequired(r.text[:200])
    d = r.json() if r.content else {}
    fails = d.get("failures") or []
    ok = d.get("successCount", len(member_keys) - len(fails)) if r.status_code == 200 else 0
    return ok, fails or ([] if r.status_code == 200 else [r.text[:200]])


def release_members(client: httpx.Client, cafe_id: int, member_keys: list[str]) -> tuple[int, list]:
    r = client.post(f"{cafe_api.STUDIO_API}/cafes/{cafe_id}/activity-stop-members/release",
                    json={"memberKeys": member_keys}, headers=cafe_api._studio_headers())
    d = r.json() if r.content else {}
    fails = d.get("failures") or []
    return (d.get("successCount", len(member_keys) - len(fails)) if r.status_code == 200 else 0), fails


def ensure_tables(db: Database):
    db.conn.executescript("""
    CREATE TABLE IF NOT EXISTS scam_action (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cafe_id INTEGER, article_id INTEGER, menu_id INTEGER,
        title TEXT, writer_nickname TEXT, writer_key TEXT,
        member_keys TEXT, reasons TEXT, snapshot TEXT,
        deleted INTEGER DEFAULT 0, stopped INTEGER DEFAULT 0, released INTEGER DEFAULT 0,
        result TEXT, created_at INTEGER,
        UNIQUE(cafe_id, article_id)
    );
    CREATE TABLE IF NOT EXISTS guard_stop (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cafe_id INTEGER, member_key TEXT, nickname TEXT, reason TEXT,
        released INTEGER DEFAULT 0, created_at INTEGER,
        UNIQUE(cafe_id, member_key)
    );
    """)
    db.conn.commit()


LAST_BEAT = 0.0     # 마지막으로 한 바퀴 끝낸 시각(메모리). server.scam_status 가 읽는다


def _errlog(where: str, e: Exception):
    """콘솔을 못 보는 환경에서도 감시 오류 원인을 남긴다(data/guard_error.log)."""
    import traceback
    try:
        with open(Path(__file__).resolve().parent.parent / "data" / "guard_error.log", "a", encoding="utf-8") as f:
            f.write(time.strftime("%m-%d %H:%M:%S") + f" [{where}] " + "".join(traceback.format_exception(e))[-900:] + chr(10))
    except Exception:
        pass


class ScamGuard:
    def __init__(self, watcher):
        self.w = watcher
        self.thread: threading.Thread | None = None
        self.gen = 0                 # 감시 스레드 세대(워치독이 새로 띄우면 올라가고, 옛 스레드는 스스로 끝낸다)
        self.beat = time.time()      # 마지막으로 한 바퀴 끝낸 시각(메모리 기준, DB 와 무관)
        self.seen: dict[tuple[int, int], dict] = {}   # 최근 글 캐시(본문)
        from .link_guard import LinkGuard
        self.links = LinkGuard(watcher)

    def start(self):
        if self.thread is not None and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._loop, args=(self.gen,), name="scam-guard", daemon=True)
        self.thread.start()
        threading.Thread(target=self._watchdog, name="scam-watchdog", daemon=True).start()
        # 링크 댓글 감시는 따로 돌린다(같은 바퀴에 끼면 등업글 확인이 7초로 늘어났음)
        self.link_thread = threading.Thread(target=self._link_loop, name="link-guard", daemon=True)
        self.link_thread.start()

    def _link_loop(self):
        time.sleep(10)
        db = Database(self.w.db.path)
        db.conn.execute("PRAGMA busy_timeout=15000")
        self.w.log(f"  🔗 광고 링크 댓글 감시 시작 (별도 스레드, {LINK_INTERVAL_S}초 간격)")
        while True:
            t0 = time.time()
            try:
                db.conn.rollback()
            except Exception:
                pass
            client = httpx.Client(headers=self.w.client.headers, cookies=self.w.client.cookies,
                                  timeout=12.0, follow_redirects=True)
            try:
                self.links.tick(db, client)
            except Exception as e:
                self.w.log(f"  ! 광고링크 댓글 감시 실패: {e}")
            finally:
                client.close()
            time.sleep(max(1, LINK_INTERVAL_S - (time.time() - t0)))

    def _staff_keys(self, db, cafe_id: int) -> set:
        """스탭·매니저 등급 계정의 member_key(5분 캐시). 우리가 올리는 글은 이 계정들이라 사칭 검사에서 뺀다."""
        c = getattr(self, "_staff_cache", {})
        hit = c.get(cafe_id)
        if hit and time.time() - hit[0] < 300:
            return hit[1]
        ks = {r[0] for r in db.conn.execute(
            "SELECT member_key FROM member_cache WHERE cafe_id=? AND (level_name LIKE '%스탭%' OR level_name LIKE '%매니저%' OR level_name LIKE '%운영%')",
            (cafe_id,))}
        c[cafe_id] = (time.time(), ks)
        self._staff_cache = c
        return ks

    def _watchdog(self):
        """감시 스레드가 죽었거나 90초 넘게 한 바퀴도 못 끝내면 새로 띄운다."""
        while True:
            time.sleep(20)
            try:
                dead = self.thread is None or not self.thread.is_alive()
                stale = time.time() - self.beat > 90
                if dead or stale:
                    self.gen += 1
                    self.beat = time.time()
                    self.w.log(f"  ⚠ 사기글 감시 {'스레드 종료' if dead else '정체'} 감지 — 새로 시작(세대 {self.gen})")
                    _errlog("watchdog", RuntimeError("dead" if dead else "stale"))
                    self.thread = threading.Thread(target=self._loop, args=(self.gen,), name="scam-guard", daemon=True)
                    self.thread.start()
            except Exception as e:
                _errlog("watchdog", e)

    def _loop(self, gen: int = 0):
        time.sleep(5)
        db = Database(self.w.db.path)
        db.conn.execute("PRAGMA busy_timeout=15000")
        ensure_tables(db)
        self.w.log(f"  🛡 등업게시판 사기글 감시 시작 ({INTERVAL_S}초 간격: "
                   + ", ".join(n for _, _, n in GUARD_BOARDS) + ")")
        fails = 0
        while gen == self.gen:
            t0 = time.time()
            err = False
            try:
                db.conn.rollback()      # 지난 바퀴에 남은 읽기 스냅샷이 쓰기를 막지 않게(WAL 에서 'database is locked' 반복의 원인)
            except Exception:
                pass
            client = httpx.Client(headers=self.w.client.headers, cookies=self.w.client.cookies,
                                  timeout=12.0, follow_redirects=True)
            try:
                for cafe_id, menu_id, name in GUARD_BOARDS:
                    try:
                        self._check_board(db, client, cafe_id, menu_id, name)
                    except Exception as e:
                        err = True
                        self.w.log(f"  ! 사기글 감시 실패 [{name}]: {e}")
                        _errlog(name, e)
                self.beat = time.time()
                global LAST_BEAT
                LAST_BEAT = self.beat          # 화면 표시는 메모리 기준(DB 가 다른 쓰기에 잠겨도 멈춤으로 보이지 않게)
                try:
                    db.set_meta("scam_guard_at", str(int(time.time())))
                except sqlite3.OperationalError as e:
                    _errlog("heartbeat", e)    # database is locked — 연결을 새로 열어 다음 바퀴에 다시 적는다
                    try:
                        db.close()
                    except Exception:
                        pass
                    db = Database(self.w.db.path)
                    db.conn.execute("PRAGMA busy_timeout=15000")
            except Exception as e:
                self.w.log(f"  ! 사기글 감시 루프 오류: {e}")
                _errlog("loop", e)
            finally:
                client.close()
            fails = fails + 1 if err else 0
            if fails >= 3:
                self.w.log(f"  ! 사기글 감시 연속 실패 {fails}회 — {BACKOFF_S}초 쉬었다 재시도")
                time.sleep(BACKOFF_S)
                continue
            time.sleep(max(1, INTERVAL_S - (time.time() - t0)))

    def _check_board(self, db, client, cafe_id, menu_id, name):
        arts = cafe_api.fetch_article_list(cafe_id, menu_id=menu_id, per_page=20, client=client)
        now = time.time() * 1000
        live = set()
        for a in arts:
            if a.is_notice or now - a.write_ts > RECHECK_MIN * 60000:
                continue
            k = (cafe_id, a.article_id)
            live.add(k)
            if db.conn.execute("SELECT 1 FROM scam_action WHERE cafe_id=? AND article_id=?", k).fetchone():
                continue
            ent = self.seen.get(k)
            if ent is None:
                try:
                    b = cafe_api.fetch_article_body(cafe_id, a.article_id, menu_id=menu_id, client=client)
                except cafe_api.ArticleGoneError:
                    continue
                # 본문이 배너 이미지뿐인 글도 있어 링크 주소를 본문에 붙여 검사
                hrefs = " ".join(re.findall(r'href="([^"]+)"', b.content_html))
                ent = self.seen[k] = {"tlen": len(b.content_text or ""), "img": "<img" in (b.content_html or ""),
                                      "title": b.title, "body": b.content_text + chr(10) + hrefs,
                                      "writer_key": b.member_key or a.member_key,
                                      "nick": b.writer_nickname, "cc": -1,
                                      "html": b.content_html, "write_ts": b.write_ts}
                time.sleep(0.4)
            comments = []
            if a.comment_count != ent["cc"]:
                comments = ent["comments"] = cafe_api.fetch_comments(cafe_id, a.article_id, client=client, max_pages=3)
                ent["cc"] = a.comment_count
            else:
                comments = ent.get("comments", [])
                time.sleep(0.4)
            s = score(ent["title"], ent["body"], comments)
            if not s["hit"] and ent["writer_key"] and ent["writer_key"] in bad_keys(db, cafe_id):
                s["hit"] = True
                s["reasons"].append("이전에 사기글을 쓴 계정")
            if not s["hit"] and ent["writer_key"] not in self._staff_keys(db, cafe_id):
                ns = nick_suspect(ent["nick"], ent["title"], ent.get("tlen", 999), ent.get("img", False))
                if ns:
                    s["hit"] = True
                    s["reasons"].append(ns)
            if s["hit"]:
                self._act(db, client, cafe_id, menu_id, name, a.article_id, ent, comments, s)
        for k in [k for k in self.seen if k[0] == cafe_id and k not in live]:
            self.seen.pop(k, None)

    def _act(self, db, client, cafe_id, menu_id, name, article_id, ent, comments, s):
        keys = accomplices(ent["writer_key"], comments)
        # 삭제하면 원문을 다시 볼 수 없으므로 원본 HTML·작성 정보까지 지우기 전에 저장한다
        snap = {"title": ent["title"], "body": ent["body"][:20000], "html": ent.get("html", ""),
                "write_ts": ent.get("write_ts"), "writer": ent["nick"], "writer_key": ent["writer_key"],
                "comments": [{"nick": c.writer_nickname, "key": c.member_key, "text": c.content[:500]}
                             for c in comments], "score": s}
        deleted, msg = delete_article(client, cafe_id, article_id)
        stopped, fails = 0, []
        if keys:
            try:
                stopped, fails = stop_members(client, cafe_id, keys)
            except cafe_api.StudioAuthRequired:
                fails = ["카페 스튜디오 본인인증 필요"]
            except Exception as e:
                fails = [f"{type(e).__name__}: {e}"]
        result = f"{msg} · 정지 {stopped}/{len(keys)}" + (f" · 실패 {fails}" if fails else "")
        db.conn.execute(
            """INSERT OR IGNORE INTO scam_action(cafe_id, article_id, menu_id, title, writer_nickname,
                   writer_key, member_keys, reasons, snapshot, deleted, stopped, result, created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (cafe_id, article_id, menu_id, ent["title"], ent["nick"], ent["writer_key"],
             json.dumps(keys), "; ".join(s["reasons"]), json.dumps(snap, ensure_ascii=False),
             int(deleted), stopped, result, int(time.time() * 1000)))
        db.conn.commit()
        self.w.log(f"  🛡 사기글 차단 [{name}][{article_id}] {ent['title'][:30]} — {result}")
        self.w._emit("scam_blocked", {"cafe_id": cafe_id, "cafe_name": name, "article_id": article_id,
                                      "title": ent["title"], "result": result})
