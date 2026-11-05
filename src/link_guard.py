"""광고 링크 댓글 차단 (2026-11-05).

이사·보험·인테리어 질문글에 '비교사이트에서 알아보세요 https://da24.co.kr/?mda=…',
'10초면 견적 https://astorie.info/…' 같은 유입 댓글을 다는 계정.
도메인을 바꿔 가며(.info 돌려막기) 같은 사람이 계속 단다.

- 마산맘·김해줌마 최신글 목록을 30초마다 보고 댓글 수가 바뀐 글의 댓글을 읽는다
  (메인 감시가 나중에 모은 댓글도 DB 에서 같이 훑는다)
- 차단 도메인(link_block) 또는 .info 링크 댓글 → 그 계정 활동정지 + 그 계정의 광고 링크 댓글 전부 삭제
- 업체 등록 계정·게시판 스탭·상생업체·운영진은 건드리지 않는다
- 차단 후보(같은 외부 도메인을 3개 글 이상에 단 계정)는 화면에만 보여 주고 사람이 도메인을 추가한다
"""
from __future__ import annotations

import json
import re
import time
from collections import defaultdict

import httpx

from . import cafe_api
from .db import Database
from .scam_guard import stop_members

CAFES = [(14793916, "마산맘"), (21031223, "김해줌마")]
SEED_BLOCK = {
    "da24.co.kr": "이사 비교 제휴링크(유주엄미)",
    "replyalba.com": "댓글 알바 모집",
}
URL_RE = re.compile(r"(?:https?://)?((?:[a-z0-9-]+\.)+[a-z]{2,})(/[^\s]*)?", re.I)
EXEMPT_LEVEL_RE = re.compile(r"스탭|상생|매니저|운영")
# 회원끼리 흔히 공유하는 링크(핫딜·이벤트·지도) — 후보 집계에서 뺀다
SHARE_RE = re.compile(
    r"(^|\.)(naver\.com|naver\.me|youtube\.com|youtu\.be|instagram\.com|kakao\.com|daum\.net|"
    r"go\.kr|or\.kr|ac\.kr|toss\.[a-z]+|tossbank\.com|kakaopay\.com|kakaobank\.io|coupang\.com|"
    r"cjonstyle\.com|11st\.co\.kr|gmarket\.co\.kr|auction\.co\.kr|ssg\.com|ssg\.li|lotteon\.com|"
    r"musinsa\.com|oliveyoung\.co\.kr|facebook\.com|x\.com|daangn\.com|kko\.to)$")


def ensure_tables(db: Database):
    db.conn.executescript("""
    CREATE TABLE IF NOT EXISTS link_block (
        domain TEXT PRIMARY KEY, note TEXT, added_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS link_spam_action (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cafe_id INTEGER, member_key TEXT, nickname TEXT, domains TEXT,
        deleted INTEGER DEFAULT 0, delete_fail INTEGER DEFAULT 0,
        stopped INTEGER DEFAULT 0, released INTEGER DEFAULT 0,
        detail TEXT, created_at INTEGER, updated_at INTEGER,
        UNIQUE(cafe_id, member_key)
    );
    """)
    for d, note in SEED_BLOCK.items():
        db.conn.execute("INSERT OR IGNORE INTO link_block VALUES(?,?,?)", (d, note, int(time.time() * 1000)))
    db.conn.commit()


def domains_in(text: str) -> list[str]:
    out = []
    for m in URL_RE.finditer(text or ""):
        d = m.group(1).lower()
        if d.startswith("www."):
            d = d[4:]
        if re.fullmatch(r"[\d.]+", d) or not (m.group(0).lower().startswith("http") or m.group(2)):
            continue
        out.append(d)
    return out


def blocked_domain(text: str, blocklist: set[str]) -> str | None:
    for d in domains_in(text):
        if d.endswith(".info") or any(d == b or d.endswith("." + b) for b in blocklist):
            return d
    return None


def delete_comment(client: httpx.Client, cafe_id: int, menu_id: int, article_id: int,
                   comment_id: int) -> bool:
    H = {"Referer": f"https://m.cafe.naver.com/ca-fe/web/cafes/{cafe_id}/articles/{article_id}",
         "Origin": "https://m.cafe.naver.com", "X-Cafe-Product": "mobile"}
    r = client.post("https://apis.naver.com/cafe-web/cafe-mobile/CommentDelete.json",
                    data={"cafeId": cafe_id, "menuId": menu_id, "articleId": article_id,
                          "commentId": comment_id, "requestFrom": "A"}, headers=H)
    return r.status_code == 200 and '"status":"200"' in r.text.replace(" ", "")


class LinkGuard:
    def __init__(self, watcher):
        self.w = watcher
        self.cc: dict[tuple[int, int], int] = {}     # 글별 마지막 댓글 수
        self.db_mark = 0

    def tick(self, db: Database, client: httpx.Client):
        ensure_tables(db)
        block = {r[0] for r in db.conn.execute("SELECT domain FROM link_block")}
        hits: dict[tuple[int, str], dict] = {}
        for cafe_id, name in CAFES:
            arts = cafe_api.fetch_article_list(cafe_id, menu_id=0, per_page=50, client=client)
            for a in arts:
                k = (cafe_id, a.article_id)
                if self.cc.get(k) == a.comment_count:
                    continue
                first = k not in self.cc
                self.cc[k] = a.comment_count
                if first and not self.db_mark:      # 시작 직후 첫 바퀴는 기준만 잡는다
                    continue
                if not a.comment_count:
                    continue
                for c in cafe_api.fetch_comments(cafe_id, a.article_id, client=client, max_pages=5):
                    d = None if c.is_deleted else blocked_domain(c.content, block)
                    if d and c.member_key:
                        h = hits.setdefault((cafe_id, c.member_key),
                                            {"nick": c.writer_nickname, "domains": set(), "extra": {}})
                        h["domains"].add(d)
                        h["extra"][(a.article_id, c.comment_id)] = a.menu_id
                time.sleep(0.4)
            if len(self.cc) > 3000:
                self.cc = dict(list(self.cc.items())[-1500:])
        # 메인 감시가 모은 댓글(재방문 등)
        since = self.db_mark or int(time.time() * 1000) - 3600 * 1000
        for cafe_id, mk, nick, content in db.conn.execute(
                f"""SELECT cafe_id, member_key, writer_nickname, content FROM comments
                    WHERE cafe_id IN ({",".join(str(c) for c, _ in CAFES)}) AND crawled_at > ?
                      AND is_deleted=0 AND content LIKE '%.%'""", (since,)).fetchall():
            d = blocked_domain(content, block)
            if d and mk:
                hits.setdefault((cafe_id, mk), {"nick": nick, "domains": set(), "extra": {}})["domains"].add(d)
        self.db_mark = int(time.time() * 1000)
        for (cafe_id, mk), h in hits.items():
            self.punish(db, client, cafe_id, mk, h["nick"], h["domains"], block, h["extra"])

    def exempt(self, db: Database, cafe_id: int, mk: str) -> str | None:
        if db.conn.execute("SELECT 1 FROM company_member WHERE cafe_id=? AND member_key=? AND status='resolved'",
                           (cafe_id, mk)).fetchone():
            return "업체 등록 계정"
        r = db.conn.execute("SELECT level_name FROM member_cache WHERE cafe_id=? AND member_key=?",
                            (cafe_id, mk)).fetchone()
        if r and EXEMPT_LEVEL_RE.search(r[0] or ""):
            return r[0]
        return None

    def punish(self, db, client, cafe_id, mk, nick, domains, block, extra=None):
        if self.exempt(db, cafe_id, mk):
            return
        prev = db.conn.execute("SELECT id, stopped, released FROM link_spam_action WHERE cafe_id=? AND member_key=?",
                               (cafe_id, mk)).fetchone()
        if prev and prev["released"]:          # 사람이 해제한 계정은 다시 건드리지 않는다
            return
        # 이 계정이 단 광고 링크 댓글 전부(DB 기록 기준) 삭제
        rows = db.conn.execute(
            """SELECT c.article_id, c.comment_id, c.content, a.menu_id FROM comments c
               LEFT JOIN articles a ON a.cafe_id=c.cafe_id AND a.article_id=c.article_id
               WHERE c.cafe_id=? AND c.member_key=? AND c.is_deleted=0""", (cafe_id, mk)).fetchall()
        targets = {(r["article_id"], r["comment_id"]): r["menu_id"] for r in rows
                   if blocked_domain(r["content"], block)}
        for k, m in (extra or {}).items():      # 방금 목록에서 본 댓글(아직 DB 에 없을 수 있음)
            targets.setdefault(k, m)
        ok = fail = 0
        done = []
        by_art = defaultdict(list)
        for (aid, cid), menu_id in targets.items():
            try:
                if menu_id is None:
                    menu_id = cafe_api.fetch_article_body(cafe_id, aid, client=client).menu_id
                delete_comment(client, cafe_id, menu_id, aid, cid)
                by_art[aid].append(cid)
            except cafe_api.ArticleGoneError:
                pass
            except Exception:
                fail += 1
            time.sleep(0.5)
        # 응답은 실패해도 200 이라 댓글을 다시 읽어 확인
        for aid, cids in by_art.items():
            try:
                live = {c.comment_id for c in cafe_api.fetch_comments(cafe_id, aid, client=client, max_pages=10)
                        if not c.is_deleted}
            except Exception:
                live = set(cids)
            for cid in cids:
                if cid in live:
                    fail += 1
                else:
                    ok += 1
                    done.append([aid, cid])
                    db.conn.execute("UPDATE comments SET is_deleted=1 WHERE cafe_id=? AND article_id=? AND comment_id=?",
                                    (cafe_id, aid, cid))
            time.sleep(0.4)
        stopped = prev["stopped"] if prev else 0
        note = ""
        if not stopped:
            try:
                s, fails = stop_members(client, cafe_id, [mk])
                stopped = int(s > 0)
                note = f" 정지실패 {fails}" if fails else ""
            except cafe_api.StudioAuthRequired:
                note = " 정지실패(스튜디오 본인인증 필요)"
        now = int(time.time() * 1000)
        doms = ", ".join(sorted(domains))
        if prev:
            db.conn.execute("""UPDATE link_spam_action SET deleted=deleted+?, delete_fail=?, stopped=?,
                                   domains=?, updated_at=? WHERE id=?""",
                            (ok, fail, stopped, doms, now, prev["id"]))
        else:
            db.conn.execute("""INSERT INTO link_spam_action(cafe_id, member_key, nickname, domains, deleted,
                                   delete_fail, stopped, detail, created_at, updated_at)
                               VALUES(?,?,?,?,?,?,?,?,?,?)""",
                            (cafe_id, mk, nick, doms, ok, fail, stopped, json.dumps(done), now, now))
        db.conn.commit()
        if ok or fail or not prev:
            name = dict(CAFES).get(cafe_id, cafe_id)
            self.w.log(f"  🔗 광고링크 댓글 차단 [{name}] {nick} ({doms}) — 댓글 삭제 {ok}"
                       + (f" 실패 {fail}" if fail else "") + f" · 정지 {'O' if stopped else 'X'}{note}")


def candidates(db: Database, days: int = 60) -> list[dict]:
    """차단 후보: 같은 외부 도메인을 3개 글 이상에 단 계정(면제·공유링크·이미 차단 제외)."""
    ensure_tables(db)
    block = {r[0] for r in db.conn.execute("SELECT domain FROM link_block")}
    since = int((time.time() - days * 86400) * 1000)
    agg = defaultdict(lambda: defaultdict(set))
    nick = {}
    for cafe_id, aid, mk, nk, ct in db.conn.execute(
            f"""SELECT cafe_id, article_id, member_key, writer_nickname, content FROM comments
                WHERE cafe_id IN ({",".join(str(c) for c, _ in CAFES)}) AND crawled_at > ?
                  AND content LIKE '%.%'""", (since,)):
        if not mk:
            continue
        for d in domains_in(ct):
            if SHARE_RE.search(d) or blocked_domain("http://" + d, block):
                continue
            agg[(cafe_id, mk)][d].add(aid)
            nick[(cafe_id, mk)] = nk
    g = LinkGuard.__new__(LinkGuard)
    out = []
    for (cafe_id, mk), ds in agg.items():
        d, arts = max(ds.items(), key=lambda x: len(x[1]))
        if len(arts) < 3 or g.exempt(db, cafe_id, mk):
            continue
        out.append({"cafe_id": cafe_id, "cafe_name": dict(CAFES)[cafe_id], "member_key": mk,
                    "nickname": nick[(cafe_id, mk)], "domain": d, "articles": len(arts),
                    "domains": {k: len(v) for k, v in ds.items()}})
    return sorted(out, key=lambda x: -x["articles"])
