"""SQLite persistence — 카페 게시글 감시 시스템."""
from __future__ import annotations

import hashlib
import re
import sqlite3
import time
from pathlib import Path


def normalize_title(title: str) -> str:
    """제목 정규화: 공백·특수문자 제거, 소문자."""
    if not title:
        return ""
    return re.sub(r"[^\w가-힣]", "", title).lower()


# 중복(도배) 판정 대상이 되려면 본문이 이 길이(정규화 후) 이상이어야 함.
# 짧은 인사글("굿모닝" 등)이 우연히 같아도 도배로 오탐하지 않도록 하는 하한.
DUP_MIN_CONTENT_LEN = 30


def content_hash(content_text: str) -> str | None:
    """중복 판정용 본문 해시. 공백·특수문자 제거 후 정규화.
    본문이 너무 짧으면(도배 판정 부적합) None."""
    if not content_text:
        return None
    norm = re.sub(r"[^\w가-힣]", "", content_text).lower()
    if len(norm) < DUP_MIN_CONTENT_LEN:
        return None
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:32]

# 재방문 체크포인트 (초 단위). H=시간.
# 일반글: 10일까지 6회. 업체글: 30일까지 12회(더 촘촘 + 연장).
REVISIT_PHASES_NORMAL: dict[str, int] = {
    "12h":  12 * 3600,
    "1d":   24 * 3600,
    "2d":   48 * 3600,
    "3d":   72 * 3600,
    "5d":  120 * 3600,
    "10d": 240 * 3600,
}
REVISIT_PHASES_COMPANY: dict[str, int] = {
    "6h":    6 * 3600,
    "12h":  12 * 3600,
    "1d":   24 * 3600,
    "2d":   48 * 3600,
    "3d":   72 * 3600,
    "4d":   96 * 3600,
    "5d":  120 * 3600,
    "7d":  168 * 3600,
    "10d": 240 * 3600,
    "14d": 336 * 3600,
    "21d": 504 * 3600,
    "30d": 720 * 3600,
}
# 하위호환: 기존 코드가 참조하던 이름
REVISIT_PHASES = REVISIT_PHASES_NORMAL

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    cafe_id             INTEGER NOT NULL,
    article_id          INTEGER NOT NULL,
    menu_id             INTEGER,
    menu_name           TEXT,
    title               TEXT,
    current_title       TEXT,
    writer_nickname     TEXT,
    naver_id            TEXT,
    member_key          TEXT,
    member_tag          TEXT,
    member_level        TEXT,
    company_name        TEXT,
    write_ts            INTEGER,
    first_seen_at       INTEGER,
    cur_read            INTEGER DEFAULT 0,
    cur_comment         INTEGER DEFAULT 0,
    cur_like            INTEGER DEFAULT 0,
    cur_snapshot_at     INTEGER,
    content_text        TEXT,
    content_html        TEXT,
    body_crawled        INTEGER DEFAULT 0,
    status              TEXT DEFAULT 'active',
    title_changed       INTEGER DEFAULT 0,
    has_keyword_hit     INTEGER DEFAULT 0,
    PRIMARY KEY (cafe_id, article_id)
);

CREATE TABLE IF NOT EXISTS title_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cafe_id         INTEGER NOT NULL,
    article_id      INTEGER NOT NULL,
    old_title       TEXT,
    new_title       TEXT,
    changed_at      INTEGER,
    check_phase     TEXT
);

CREATE TABLE IF NOT EXISTS revisit_schedule (
    cafe_id         INTEGER NOT NULL,
    article_id      INTEGER NOT NULL,
    phase           TEXT NOT NULL,
    due_at          INTEGER NOT NULL,
    done            INTEGER DEFAULT 0,
    checked_at      INTEGER,
    PRIMARY KEY (cafe_id, article_id, phase)
);

CREATE TABLE IF NOT EXISTS keywords (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword     TEXT NOT NULL UNIQUE,
    is_active   INTEGER DEFAULT 1,
    created_at  INTEGER
);

CREATE TABLE IF NOT EXISTS keyword_hits (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword_id   INTEGER,
    keyword_text TEXT,
    cafe_id      INTEGER NOT NULL,
    article_id   INTEGER NOT NULL,
    hit_location TEXT,
    hit_snippet  TEXT,
    found_at     INTEGER,
    check_phase  TEXT
);

CREATE TABLE IF NOT EXISTS company_mapping (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    naver_id     TEXT NOT NULL UNIQUE,
    company_name TEXT NOT NULL,
    synced_at    INTEGER
);

-- 구글시트 원본 (B열=naver_id, C열=company_name)
CREATE TABLE IF NOT EXISTS company_master (
    naver_id     TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    row_index    INTEGER,          -- 시트 행 번호 (증분 동기화용)
    added_at     INTEGER
);

-- 카페별 회원 해석 결과 (member_key/tag는 카페마다 다름)
CREATE TABLE IF NOT EXISTS company_member (
    naver_id     TEXT NOT NULL,
    cafe_id      INTEGER NOT NULL,
    member_key   TEXT,
    member_tag   TEXT,
    nickname     TEXT,
    masked_id    TEXT,
    level_name   TEXT,
    company_name TEXT,
    status       TEXT DEFAULT 'pending',   -- pending | resolved | not_member
    resolved_at  INTEGER,
    PRIMARY KEY (naver_id, cafe_id)
);

-- 작성자 멤버태그 캐시 (member_key → member_tag, 카페별 고정이라 1회 조회 후 재사용)
CREATE TABLE IF NOT EXISTS member_cache (
    cafe_id     INTEGER NOT NULL,
    member_key  TEXT NOT NULL,
    member_tag  TEXT,
    nickname    TEXT,
    masked_id   TEXT,
    level_name  TEXT,
    cached_at   INTEGER,
    resolved    INTEGER DEFAULT 0,   -- 1=조회성공, 0=조회실패(닉네임변경/탈퇴 등)
    PRIMARY KEY (cafe_id, member_key)
);

-- 글관리: 작성자 그룹 (등록 작성자를 그룹으로 묶어 관리)
CREATE TABLE IF NOT EXISTS writer_group (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    sort_order  INTEGER DEFAULT 0,
    created_at  INTEGER
);

-- 서포터즈 분석: 그룹
CREATE TABLE IF NOT EXISTS supporter_group (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    created_at  INTEGER
);

-- 서포터즈 분석: 그룹원 (카페별 member_key 단위)
CREATE TABLE IF NOT EXISTS supporter_member (
    group_id    INTEGER NOT NULL,
    cafe_id     INTEGER NOT NULL,
    member_key  TEXT NOT NULL,
    naver_id    TEXT,
    nickname    TEXT,
    member_tag  TEXT,
    masked_id   TEXT,
    added_at    INTEGER,
    PRIMARY KEY (group_id, cafe_id, member_key)
);

-- 네이버 카페 통계도구(멤버순위) 월간 공식 통계 캐시 (삭제 포함 누적)
CREATE TABLE IF NOT EXISTS naver_member_stat (
    cafe_id     INTEGER NOT NULL,
    year_month  TEXT NOT NULL,       -- 'YYYY-MM'
    user_id     TEXT NOT NULL,       -- 실제 네이버 로그인 아이디
    nickname    TEXT,
    level_name  TEXT,
    join_date   TEXT,
    n_create    INTEGER,             -- 작성 게시글수
    n_comment   INTEGER,             -- 작성 댓글수
    n_like_sent INTEGER,             -- 보낸 좋아요수
    n_like_recv INTEGER,             -- 받은 좋아요수
    n_view      INTEGER,             -- 조회수
    n_visit     INTEGER,             -- 방문횟수
    updated_at  INTEGER,
    PRIMARY KEY (cafe_id, year_month, user_id)
);

-- 글관리: 게시글을 추적할 등록 작성자 (카페별 member_key 단위)
CREATE TABLE IF NOT EXISTS watched_writer (
    cafe_id      INTEGER NOT NULL,
    member_key   TEXT NOT NULL,
    naver_id     TEXT,
    nickname     TEXT,
    masked_id    TEXT,
    member_tag   TEXT,
    level_name   TEXT,
    memo         TEXT,
    registered_at INTEGER,
    PRIMARY KEY (cafe_id, member_key)
);

CREATE TABLE IF NOT EXISTS comments (
    cafe_id         INTEGER NOT NULL,
    article_id      INTEGER NOT NULL,
    comment_id      INTEGER NOT NULL,
    ref_id          INTEGER,
    writer_nickname TEXT,
    naver_id        TEXT,
    member_key      TEXT,
    content         TEXT,
    update_ts       INTEGER,
    is_reply        INTEGER DEFAULT 0,
    is_deleted      INTEGER DEFAULT 0,
    phase           TEXT,
    crawled_at      INTEGER,
    PRIMARY KEY (cafe_id, article_id, comment_id, phase)
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_articles_write_ts ON articles (write_ts DESC);
CREATE INDEX IF NOT EXISTS idx_articles_status ON articles (status);
CREATE INDEX IF NOT EXISTS idx_articles_company ON articles (company_name);
CREATE INDEX IF NOT EXISTS idx_revisit_due ON revisit_schedule (done, due_at);
CREATE INDEX IF NOT EXISTS idx_keyword_hits_article ON keyword_hits (cafe_id, article_id);
CREATE INDEX IF NOT EXISTS idx_title_history_article ON title_history (cafe_id, article_id);
CREATE INDEX IF NOT EXISTS idx_company_member_key ON company_member (cafe_id, member_key);
CREATE INDEX IF NOT EXISTS idx_company_member_status ON company_member (status);
CREATE INDEX IF NOT EXISTS idx_articles_member ON articles (cafe_id, member_key);
"""


def now_ms() -> int:
    return int(time.time() * 1000)


class Database:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self):
        """기존 DB에 없는 컬럼 추가."""
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(articles)")}
        new_flag_ts = "flag_ts" not in cols
        for col, decl in (("member_tag", "TEXT"), ("member_level", "TEXT"),
                          ("reviewed", "INTEGER DEFAULT 0"), ("reviewed_at", "INTEGER"),
                          ("deleted_at", "INTEGER"), ("flag_ts", "INTEGER"),
                          ("norm_title", "TEXT"), ("is_duplicate", "INTEGER DEFAULT 0"),
                          ("dup_count", "INTEGER DEFAULT 0"), ("content_hash", "TEXT"),
                          ("dup_rep", "INTEGER DEFAULT 1"),  # 중복그룹 대표(최신)=1, 나머지=0

                          ("delete_type", "TEXT"),          # staff | self | NULL(미정)
                          ("deleted_by_nick", "TEXT"), ("deleted_by_tag", "TEXT"),
                          ("deleted_by_id", "TEXT"),
                          ("image_count", "INTEGER"), ("content_len", "INTEGER")):
            if col not in cols:
                self.conn.execute(f"ALTER TABLE articles ADD COLUMN {col} {decl}")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_dup ON articles (cafe_id, member_key, content_hash)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_dupgrp ON articles (cafe_id, member_key, content_hash, write_ts)")
        # 카페+작성자 단위 중복 대표 계산용 (계정당 최신 1건)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_memwt ON articles (cafe_id, member_key, write_ts, article_id)")
        # 이상감지 여부 생성컬럼(제목변경|삭제|키워드|중복) — 우측 패널 조회를 O(전체스캔)에서
        # 인덱스 조회로 전환(중복 폭증 시 상관 서브쿼리로 멈추던 문제 근본 해결).
        try:
            self.conn.execute(
                "ALTER TABLE articles ADD COLUMN is_flagged AS "
                "(CASE WHEN title_changed=1 OR status='deleted' OR has_keyword_hit=1 "
                "OR is_duplicate=1 THEN 1 ELSE 0 END) VIRTUAL")
        except Exception:
            pass  # 이미 존재
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_is_flagged ON articles (is_flagged, reviewed)")
        # 작성자 그룹: 기존 watched_writer 에 group_id 추가
        ww_cols = {r[1] for r in self.conn.execute("PRAGMA table_info(watched_writer)")}
        if "group_id" not in ww_cols:
            self.conn.execute("ALTER TABLE watched_writer ADD COLUMN group_id INTEGER")
        if new_flag_ts:
            # 기존 이상감지 게시글의 flag_ts를 이력에서 백필
            # (제목변경 최근시각 / 키워드 최근발견 / write_ts 중 최대)
            self.conn.execute("""
                UPDATE articles SET flag_ts = (
                    SELECT MAX(x) FROM (
                        SELECT (SELECT MAX(changed_at) FROM title_history t
                                WHERE t.cafe_id=articles.cafe_id AND t.article_id=articles.article_id) AS x
                        UNION ALL
                        SELECT (SELECT MAX(found_at) FROM keyword_hits k
                                WHERE k.cafe_id=articles.cafe_id AND k.article_id=articles.article_id)
                        UNION ALL
                        SELECT articles.write_ts
                    )
                )
                WHERE title_changed=1 OR status='deleted' OR has_keyword_hit=1
            """)

    def close(self):
        self.conn.close()

    # ── 게시글 ────────────────────────────────────────────────────────────

    def upsert_article(self, a, session_ok: bool = False) -> bool:
        """신규 게시글 삽입. True=신규, False=기존."""
        ts = now_ms()
        norm = normalize_title(a.title)
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO articles
               (cafe_id, article_id, menu_id, menu_name, title, current_title,
                writer_nickname, member_key, write_ts, first_seen_at,
                cur_read, cur_comment, cur_like, cur_snapshot_at, norm_title)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (a.cafe_id, a.article_id, a.menu_id, a.menu_name, a.title, a.title,
             a.writer_nickname, a.member_key, a.write_ts, ts,
             a.read_count, a.comment_count, a.like_count, ts, norm),
        )
        is_new = cur.rowcount > 0
        if is_new:
            self._schedule_revisits(a.cafe_id, a.article_id, ts)
            # 중복 판정은 본문 기준 → 본문 확보 시점(save_body)에 수행.
        self.conn.commit()
        return is_new

    def backfill_duplicates(self) -> dict:
        """본문(content_text) 해시 기반으로 중복글 일괄 재판정.
        본문이 실제로 동일한 복붙 도배만 잡는다 (제목만 같은 정상 반복글 제외)."""
        # 1) content_hash 비어있는데 본문 있는 것 채우기
        rows = self.conn.execute(
            "SELECT cafe_id, article_id, content_text FROM articles "
            "WHERE (content_hash IS NULL) AND content_text IS NOT NULL AND content_text!=''"
        ).fetchall()
        filled = 0
        for r in rows:
            ch = content_hash(r["content_text"])
            if ch:
                self.conn.execute(
                    "UPDATE articles SET content_hash=? WHERE cafe_id=? AND article_id=?",
                    (ch, r["cafe_id"], r["article_id"]),
                )
                filled += 1
        self.conn.commit()
        # 2) 초기화 후 본문 해시 그룹(같은 cafe+member+content_hash 2개 이상) 판정
        self.conn.execute("UPDATE articles SET is_duplicate=0, dup_count=0")
        self.conn.execute(
            """UPDATE articles SET is_duplicate=1, dup_count=(
                   SELECT COUNT(*) FROM articles b
                   WHERE b.cafe_id=articles.cafe_id AND b.member_key=articles.member_key
                     AND b.content_hash=articles.content_hash)
               WHERE member_key!='' AND content_hash IS NOT NULL AND (
                   SELECT COUNT(*) FROM articles b
                   WHERE b.cafe_id=articles.cafe_id AND b.member_key=articles.member_key
                     AND b.content_hash=articles.content_hash) >= 2"""
        )
        self.conn.commit()
        # 3) 대표(최신) 사전계산: 같은 카페의 '같은 작성자'가 올린 중복글은 하나로 묶어
        #    가장 최근(write_ts, 동점은 article_id) 1건만 dup_rep=1. 나머지는 우측 패널에서 숨김.
        #    (한 계정이 도배한 여러 중복글 중 최신 1건만 감시대상으로 노출 → 계정당 1줄)
        self.conn.execute("UPDATE articles SET dup_rep=1")
        self.conn.execute(
            """UPDATE articles SET dup_rep=0
               WHERE is_duplicate=1 AND EXISTS (
                   SELECT 1 FROM articles b
                   WHERE b.cafe_id=articles.cafe_id AND b.member_key=articles.member_key
                     AND b.is_duplicate=1
                     AND (b.write_ts > articles.write_ts
                          OR (b.write_ts = articles.write_ts AND b.article_id > articles.article_id)))"""
        )
        self.conn.commit()
        dup = self.conn.execute("SELECT COUNT(*) FROM articles WHERE is_duplicate=1").fetchone()[0]
        return {"hash_filled": filled, "duplicates": dup}

    def _mark_duplicates(self, cafe_id: int, member_key: str, chash: str):
        """같은 작성자가 같은 본문(해시)을 2개 이상 → 그 그룹 전부 중복 표시.
        본문 내용이 실제로 동일한 복붙 도배만 잡는다 (제목만 같은 정상 반복글 제외)."""
        if not member_key or not chash:
            return
        n = self.conn.execute(
            "SELECT COUNT(*) FROM articles WHERE cafe_id=? AND member_key=? AND content_hash=?",
            (cafe_id, member_key, chash),
        ).fetchone()[0]
        if n >= 2:
            self.conn.execute(
                """UPDATE articles SET is_duplicate=1, dup_count=?,
                   flag_ts=COALESCE(flag_ts, ?)
                   WHERE cafe_id=? AND member_key=? AND content_hash=?""",
                (n, now_ms(), cafe_id, member_key, chash),
            )
            # 대표 재계산: 같은 카페+작성자의 중복글 전체에서 가장 최근 1건만 dup_rep=1.
            # 한 계정이 도배한 여러 중복글은 우측 패널에 최신 1건만 노출한다(계정당 1줄).
            self.conn.execute(
                "UPDATE articles SET dup_rep=0 WHERE cafe_id=? AND member_key=? AND is_duplicate=1",
                (cafe_id, member_key))
            self.conn.execute(
                """UPDATE articles SET dup_rep=1
                   WHERE cafe_id=? AND member_key=? AND is_duplicate=1
                     AND NOT EXISTS (
                         SELECT 1 FROM articles b
                         WHERE b.cafe_id=articles.cafe_id AND b.member_key=articles.member_key
                           AND b.is_duplicate=1
                           AND (b.write_ts > articles.write_ts
                                OR (b.write_ts = articles.write_ts AND b.article_id > articles.article_id)))""",
                (cafe_id, member_key))

    def upsert_body_as_article(self, cafe_id: int, article_id: int, body) -> bool:
        """번호 역순 과거수집용: 본문(ArticleBody)만으로 게시글 레코드 생성.
        목록 메타가 없으므로 본문에서 얻은 정보로 저장. 이미 있으면 False."""
        ts = now_ms()
        norm = normalize_title(body.title)
        chash = content_hash(body.content_text)
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO articles
               (cafe_id, article_id, menu_id, title, current_title,
                writer_nickname, member_key, write_ts, first_seen_at,
                cur_read, cur_comment, content_text, body_crawled, norm_title, content_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)""",
            (cafe_id, article_id, body.menu_id, body.title, body.title,
             body.writer_nickname, body.member_key, body.write_ts, ts,
             body.read_count, body.comment_count, body.content_text, norm, chash),
        )
        is_new = cur.rowcount > 0
        if is_new and chash and body.member_key:
            self._mark_duplicates(cafe_id, body.member_key, chash)
        self.conn.commit()
        return is_new

    def _schedule_revisits(self, cafe_id: int, article_id: int, base_ts: int,
                           phases: dict | None = None):
        phases = phases or REVISIT_PHASES_NORMAL
        self.conn.executemany(
            """INSERT OR IGNORE INTO revisit_schedule
               (cafe_id, article_id, phase, due_at) VALUES (?,?,?,?)""",
            [(cafe_id, article_id, phase, base_ts + delay * 1000)
             for phase, delay in phases.items()],
        )

    def upgrade_to_company_schedule(self, cafe_id: int, article_id: int):
        """업체로 판명된 글에 촘촘한 업체 스케줄을 추가 삽입 (기존 것 유지).
        base_ts는 원래 감지시각(first_seen_at) 기준."""
        row = self.conn.execute(
            "SELECT first_seen_at FROM articles WHERE cafe_id=? AND article_id=?",
            (cafe_id, article_id),
        ).fetchone()
        if not row or not row["first_seen_at"]:
            return
        self._schedule_revisits(cafe_id, article_id, row["first_seen_at"],
                                phases=REVISIT_PHASES_COMPANY)
        self.conn.commit()

    def update_counts_bulk(self, arts):
        if not arts:
            return
        ts = now_ms()
        self.conn.executemany(
            """UPDATE articles SET cur_read=?, cur_comment=?, cur_like=?, cur_snapshot_at=?
               WHERE cafe_id=? AND article_id=?""",
            [(a.read_count, a.comment_count, a.like_count, ts, a.cafe_id, a.article_id)
             for a in arts],
        )
        self.conn.commit()

    def upsert_member_articles(self, arts) -> int:
        """작성자 과거글 백필: articles 재사용, 한 번의 커밋(페이지 단위).
        신규는 INSERT(재방문 스케줄 없음 — 대량 과거글이 큐를 넘치지 않게),
        기존(실시간 수집분 등)은 감지정보 보존하고 현재 카운트(cur_*)만 갱신.
        반환: 새로 추가된 글 수."""
        if not arts:
            return 0
        ts = now_ms()
        new = 0
        for a in arts:
            cur = self.conn.execute(
                """INSERT OR IGNORE INTO articles
                   (cafe_id, article_id, menu_id, menu_name, title, current_title,
                    writer_nickname, member_key, write_ts, first_seen_at,
                    cur_read, cur_comment, cur_like, cur_snapshot_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (a.cafe_id, a.article_id, a.menu_id, a.menu_name, a.title, a.title,
                 a.writer_nickname, a.member_key, a.write_ts, ts,
                 a.read_count, a.comment_count, a.like_count, ts),
            )
            if cur.rowcount > 0:
                new += 1
            else:
                self.conn.execute(
                    """UPDATE articles SET cur_read=?, cur_comment=?, cur_like=?, cur_snapshot_at=?,
                           menu_id=COALESCE(NULLIF(menu_id,0), ?),
                           menu_name=COALESCE(NULLIF(menu_name,''), ?),
                           member_key=COALESCE(NULLIF(member_key,''), ?),
                           writer_nickname=COALESCE(NULLIF(writer_nickname,''), ?)
                       WHERE cafe_id=? AND article_id=?""",
                    (a.read_count, a.comment_count, a.like_count, ts,
                     a.menu_id, a.menu_name, a.member_key, a.writer_nickname,
                     a.cafe_id, a.article_id))
        self.conn.commit()
        return new

    def save_body(self, body):
        # content_html은 저장하지 않지만(용량), 사진 수(img 태그)와 글자 수는 여기서 계산해 저장.
        chash = content_hash(body.content_text)
        img_count = len(re.findall(r"<img\b", body.content_html or "", re.I))
        clen = len(body.content_text or "")
        self.conn.execute(
            """UPDATE articles SET content_text=?, body_crawled=1, content_hash=?,
               image_count=?, content_len=?,
               menu_id=COALESCE(NULLIF(menu_id,0), ?)
               WHERE cafe_id=? AND article_id=?""",
            (body.content_text, chash, img_count, clen, body.menu_id,
             body.cafe_id, body.article_id),
        )
        self.conn.commit()
        # 본문 확보 → 같은 작성자·같은 본문(복붙) 중복 판정
        if chash:
            mk = self.conn.execute(
                "SELECT member_key FROM articles WHERE cafe_id=? AND article_id=?",
                (body.cafe_id, body.article_id),
            ).fetchone()
            if mk and mk["member_key"]:
                self._mark_duplicates(body.cafe_id, mk["member_key"], chash)

    def update_revisit_counts(self, cafe_id: int, article_id: int,
                              read_count: int, comment_count: int) -> int:
        """재방문 시 최신 조회수·댓글수 반영. 이전 댓글수와의 차이를 반환(변동 감지용)."""
        row = self.conn.execute(
            "SELECT cur_comment FROM articles WHERE cafe_id=? AND article_id=?",
            (cafe_id, article_id),
        ).fetchone()
        prev = (row["cur_comment"] if row else 0) or 0
        self.conn.execute(
            """UPDATE articles SET cur_read=?, cur_comment=?, cur_snapshot_at=?
               WHERE cafe_id=? AND article_id=?""",
            (read_count, comment_count, now_ms(), cafe_id, article_id),
        )
        self.conn.commit()
        return (comment_count or 0) - prev

    def update_naver_id(self, cafe_id: int, article_id: int, naver_id: str):
        self.conn.execute(
            "UPDATE articles SET naver_id=? WHERE cafe_id=? AND article_id=?",
            (naver_id, cafe_id, article_id),
        )
        self.conn.commit()

    def update_company(self, cafe_id: int, article_id: int, company_name: str | None):
        self.conn.execute(
            "UPDATE articles SET company_name=? WHERE cafe_id=? AND article_id=?",
            (company_name, cafe_id, article_id),
        )
        self.conn.commit()

    def update_member_info(self, cafe_id: int, article_id: int,
                           member_tag: str | None, member_level: str | None,
                           company_name: str | None):
        """게시글에 멤버태그·등급·업체명 마킹."""
        self.conn.execute(
            """UPDATE articles SET member_tag=?, member_level=?, company_name=?
               WHERE cafe_id=? AND article_id=?""",
            (member_tag, member_level, company_name, cafe_id, article_id),
        )
        self.conn.commit()

    def mark_deleted(self, cafe_id: int, article_id: int):
        ts = now_ms()
        # 새 이상 발생 → flag_ts 갱신, reviewed 리셋 (다시 미확인으로 상단 배치).
        # delete_type은 미정(NULL) — 이후 스탭삭제 동기화에서 staff/self 확정.
        self.conn.execute(
            """UPDATE articles SET status='deleted', deleted_at=COALESCE(deleted_at,?),
               flag_ts=?, reviewed=0 WHERE cafe_id=? AND article_id=?""",
            (ts, ts, cafe_id, article_id),
        )
        self.conn.execute(
            "UPDATE revisit_schedule SET done=1 WHERE cafe_id=? AND article_id=?",
            (cafe_id, article_id),
        )
        self.conn.commit()

    def apply_staff_deletion(self, ra) -> bool:
        """스탭 삭제 목록의 항목을 반영. ra=RemovedArticle.
        해당 게시글을 status='deleted', delete_type='staff' + 삭제자 기록.
        우리 DB에 있는 글만 갱신 (없으면 무시). True=갱신됨."""
        ts = now_ms()
        cur = self.conn.execute(
            """UPDATE articles SET status='deleted', delete_type='staff',
                   deleted_at=COALESCE(deleted_at, ?),
                   deleted_by_nick=?, deleted_by_tag=?, deleted_by_id=?
               WHERE cafe_id=? AND article_id=?""",
            (ts, ra.remover_nick, ra.remover_tag, ra.remover_masked_id,
             ra.cafe_id, ra.article_id),
        )
        if cur.rowcount:
            self.conn.execute(
                "UPDATE revisit_schedule SET done=1 WHERE cafe_id=? AND article_id=?",
                (ra.cafe_id, ra.article_id))
        self.conn.commit()
        return cur.rowcount > 0

    def finalize_self_deletions(self, grace_hours: int = 6) -> int:
        """삭제 감지 후 grace_hours 지났는데 아직 delete_type 미정(NULL)이면 자삭으로 확정.
        (그 시간 동안 스탭 삭제 목록에 안 나타났으면 작성자 본인 삭제로 판단)"""
        cutoff = now_ms() - grace_hours * 3600 * 1000
        cur = self.conn.execute(
            """UPDATE articles SET delete_type='self'
               WHERE status='deleted' AND delete_type IS NULL AND deleted_at <= ?""",
            (cutoff,),
        )
        self.conn.commit()
        return cur.rowcount

    def check_title_change(self, cafe_id: int, article_id: int, new_title: str, phase: str) -> bool:
        """제목 비교 후 변경 시 history 저장. True=변경됨."""
        row = self.conn.execute(
            "SELECT current_title FROM articles WHERE cafe_id=? AND article_id=?",
            (cafe_id, article_id),
        ).fetchone()
        if not row:
            return False
        old_title = row["current_title"] or ""
        if old_title == new_title:
            return False
        ts = now_ms()
        self.conn.execute(
            """INSERT INTO title_history (cafe_id, article_id, old_title, new_title, changed_at, check_phase)
               VALUES (?,?,?,?,?,?)""",
            (cafe_id, article_id, old_title, new_title, ts, phase),
        )
        # 새 이상 발생 → flag_ts 갱신, reviewed 리셋 (확인했어도 재변경 시 다시 상단)
        self.conn.execute(
            """UPDATE articles SET current_title=?, title_changed=1, flag_ts=?, reviewed=0
               WHERE cafe_id=? AND article_id=?""",
            (new_title, ts, cafe_id, article_id),
        )
        self.conn.commit()
        return True

    # ── 재방문 스케줄 ──────────────────────────────────────────────────────

    def due_revisits(self, limit: int = 50) -> list:
        # body_crawled 조건 없음: 본문 크롤이 실패했어도 제목변경/삭제는 재방문에서
        # 확인해야 한다 (재방문 시 제목을 새로 가져오므로 본문 크롤 성공 여부와 무관).
        # 업체글 우선 처리 (수정·삭제 타이밍이 더 중요).
        return self.conn.execute(
            """SELECT r.cafe_id, r.article_id, r.phase, a.menu_id, a.title,
                      a.current_title, a.member_key, a.naver_id
               FROM revisit_schedule r
               JOIN articles a ON a.cafe_id=r.cafe_id AND a.article_id=r.article_id
               WHERE r.done=0 AND r.due_at<=? AND a.status='active'
               ORDER BY (CASE WHEN a.company_name IS NOT NULL AND a.company_name!='' THEN 0 ELSE 1 END),
                        r.due_at
               LIMIT ?""",
            (now_ms(), limit),
        ).fetchall()

    def complete_revisit(self, cafe_id: int, article_id: int, phase: str):
        self.conn.execute(
            """UPDATE revisit_schedule SET done=1, checked_at=?
               WHERE cafe_id=? AND article_id=? AND phase=?""",
            (now_ms(), cafe_id, article_id, phase),
        )
        self.conn.commit()

    # ── 댓글 ──────────────────────────────────────────────────────────────

    def save_comments(self, cafe_id: int, article_id: int, comments, phase: str):
        ts = now_ms()
        self.conn.executemany(
            """INSERT OR REPLACE INTO comments
               (cafe_id, article_id, comment_id, ref_id, writer_nickname, member_key,
                content, update_ts, is_reply, is_deleted, phase, crawled_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(cafe_id, article_id, c.comment_id, c.ref_id, c.writer_nickname, c.member_key,
              c.content, c.update_ts, int(c.is_reply), int(c.is_deleted), phase, ts)
             for c in comments],
        )
        self.conn.commit()

    # ── 키워드 ────────────────────────────────────────────────────────────

    def get_active_keywords(self) -> list:
        return self.conn.execute(
            "SELECT id, keyword FROM keywords WHERE is_active=1"
        ).fetchall()

    def add_keyword(self, keyword: str) -> int:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO keywords (keyword, created_at) VALUES (?,?)",
            (keyword.strip(), now_ms()),
        )
        self.conn.commit()
        if cur.lastrowid:
            return cur.lastrowid
        row = self.conn.execute("SELECT id FROM keywords WHERE keyword=?", (keyword.strip(),)).fetchone()
        return row["id"]

    def delete_keyword(self, keyword_id: int):
        self.conn.execute("DELETE FROM keywords WHERE id=?", (keyword_id,))
        self.conn.commit()

    def toggle_keyword(self, keyword_id: int, is_active: bool):
        self.conn.execute(
            "UPDATE keywords SET is_active=? WHERE id=?",
            (1 if is_active else 0, keyword_id),
        )
        self.conn.commit()

    def save_keyword_hit(self, keyword_id: int, keyword_text: str,
                         cafe_id: int, article_id: int,
                         hit_location: str, hit_snippet: str, phase: str):
        ts = now_ms()
        self.conn.execute(
            """INSERT INTO keyword_hits
               (keyword_id, keyword_text, cafe_id, article_id, hit_location, hit_snippet, found_at, check_phase)
               VALUES (?,?,?,?,?,?,?,?)""",
            (keyword_id, keyword_text, cafe_id, article_id, hit_location, hit_snippet, ts, phase),
        )
        # 새 키워드 발견 → flag_ts 갱신, reviewed 리셋
        self.conn.execute(
            "UPDATE articles SET has_keyword_hit=1, flag_ts=?, reviewed=0 WHERE cafe_id=? AND article_id=?",
            (ts, cafe_id, article_id),
        )
        self.conn.commit()

    def keyword_hit_exists(self, keyword_id: int, cafe_id: int,
                           article_id: int, hit_location: str, phase: str) -> bool:
        r = self.conn.execute(
            """SELECT 1 FROM keyword_hits
               WHERE keyword_id=? AND cafe_id=? AND article_id=? AND hit_location=? AND check_phase=?""",
            (keyword_id, cafe_id, article_id, hit_location, phase),
        ).fetchone()
        return r is not None

    # ── 업체 매칭 ─────────────────────────────────────────────────────────

    def get_company_by_naver_id(self, naver_id: str) -> str | None:
        if not naver_id:
            return None
        row = self.conn.execute(
            "SELECT company_name FROM company_mapping WHERE naver_id=?", (naver_id,)
        ).fetchone()
        return row["company_name"] if row else None

    def upsert_company_mapping(self, naver_id: str, company_name: str):
        self.conn.execute(
            """INSERT INTO company_mapping (naver_id, company_name, synced_at)
               VALUES (?,?,?)
               ON CONFLICT(naver_id) DO UPDATE SET company_name=excluded.company_name, synced_at=excluded.synced_at""",
            (naver_id, company_name, now_ms()),
        )
        self.conn.commit()

    def get_all_company_mappings(self) -> list:
        return self.conn.execute(
            "SELECT naver_id, company_name, synced_at FROM company_mapping ORDER BY company_name"
        ).fetchall()

    # ── 업체 마스터 (구글시트 원본) ────────────────────────────────────────

    def get_master_max_row(self) -> int:
        r = self.conn.execute("SELECT MAX(row_index) FROM company_master").fetchone()
        return r[0] or 0

    def upsert_company_master(self, naver_id: str, company_name: str, row_index: int) -> bool:
        """시트 행 추가/갱신. 신규면 company_member에 pending 등록. True=신규 ID."""
        naver_id = naver_id.strip().lower()
        exists = self.conn.execute(
            "SELECT 1 FROM company_master WHERE naver_id=?", (naver_id,)
        ).fetchone()
        self.conn.execute(
            """INSERT INTO company_master (naver_id, company_name, row_index, added_at)
               VALUES (?,?,?,?)
               ON CONFLICT(naver_id) DO UPDATE SET
                 company_name=excluded.company_name, row_index=excluded.row_index""",
            (naver_id, company_name, row_index, now_ms()),
        )
        self.conn.commit()
        return not exists

    def enqueue_member_resolution(self, naver_id: str, company_name: str, cafe_ids: list[int]):
        """각 카페에 대해 pending 조회 항목 생성 (이미 있으면 유지)."""
        naver_id = naver_id.strip().lower()
        for cid in cafe_ids:
            self.conn.execute(
                """INSERT OR IGNORE INTO company_member
                   (naver_id, cafe_id, company_name, status) VALUES (?,?,?,'pending')""",
                (naver_id, cid, company_name),
            )
        self.conn.commit()

    def get_pending_resolutions(self, limit: int = 20) -> list:
        return self.conn.execute(
            "SELECT naver_id, cafe_id, company_name FROM company_member WHERE status='pending' LIMIT ?",
            (limit,),
        ).fetchall()

    def requeue_not_member(self, limit: int = 300) -> int:
        """가장 오래된 not_member 항목을 pending 으로 되돌려 재해석 대상에 넣는다.
        세션 문제 등으로 실패했던 항목이 영구 방치되지 않게 하는 자가치유용."""
        cur = self.conn.execute(
            """UPDATE company_member SET status='pending'
               WHERE rowid IN (
                   SELECT rowid FROM company_member
                   WHERE status='not_member' ORDER BY resolved_at ASC LIMIT ?)""",
            (limit,),
        )
        self.conn.commit()
        return cur.rowcount

    def save_member_resolution(self, naver_id: str, cafe_id: int, member,
                               company_name: str, status: str):
        """조회 결과 저장. member=CafeMember|None."""
        naver_id = naver_id.strip().lower()
        if member:
            self.conn.execute(
                """UPDATE company_member SET member_key=?, member_tag=?, nickname=?,
                   masked_id=?, level_name=?, company_name=?, status=?, resolved_at=?
                   WHERE naver_id=? AND cafe_id=?""",
                (member.member_key, member.member_tag, member.nickname, member.masked_id,
                 member.level_name, company_name, status, now_ms(), naver_id, cafe_id),
            )
        else:
            self.conn.execute(
                """UPDATE company_member SET company_name=?, status=?, resolved_at=?
                   WHERE naver_id=? AND cafe_id=?""",
                (company_name, status, now_ms(), naver_id, cafe_id),
            )
        self.conn.commit()
        # 해석 성공 시 이 회원이 이미 쓴 기존 게시글에 소급 매칭
        if member and member.member_key:
            self.conn.execute(
                """UPDATE articles SET company_name=?, member_tag=?, member_level=?
                   WHERE cafe_id=? AND member_key=?""",
                (company_name, member.member_tag, member.level_name, cafe_id, member.member_key),
            )
            self.conn.commit()

    def backfill_all_resolved(self) -> int:
        """resolved된 모든 업체회원 → 기존 게시글 일괄 소급 매칭 (수동 보정용)."""
        rows = self.conn.execute(
            """SELECT cafe_id, member_key, member_tag, level_name, company_name
               FROM company_member WHERE status='resolved' AND member_key IS NOT NULL"""
        ).fetchall()
        n = 0
        for r in rows:
            cur = self.conn.execute(
                """UPDATE articles SET company_name=?, member_tag=?, member_level=?
                   WHERE cafe_id=? AND member_key=?""",
                (r["company_name"], r["member_tag"], r["level_name"], r["cafe_id"], r["member_key"]),
            )
            n += cur.rowcount
        self.conn.commit()
        return n

    def get_company_by_member_key(self, cafe_id: int, member_key: str) -> dict | None:
        """게시글 작성자 member_key로 업체 조회 (핵심 매칭)."""
        if not member_key:
            return None
        row = self.conn.execute(
            """SELECT company_name, member_tag, level_name, naver_id FROM company_member
               WHERE cafe_id=? AND member_key=? AND status='resolved'""",
            (cafe_id, member_key),
        ).fetchone()
        return dict(row) if row else None

    def resolution_stats(self) -> dict:
        c = self.conn.execute
        return {
            "master": c("SELECT COUNT(*) FROM company_master").fetchone()[0],
            "resolved": c("SELECT COUNT(*) FROM company_member WHERE status='resolved'").fetchone()[0],
            "pending": c("SELECT COUNT(*) FROM company_member WHERE status='pending'").fetchone()[0],
            "not_member": c("SELECT COUNT(*) FROM company_member WHERE status='not_member'").fetchone()[0],
        }

    # ── 멤버태그 캐시 ──────────────────────────────────────────────────────

    def get_cached_member(self, cafe_id: int, member_key: str) -> dict | None:
        r = self.conn.execute(
            "SELECT * FROM member_cache WHERE cafe_id=? AND member_key=?",
            (cafe_id, member_key),
        ).fetchone()
        return dict(r) if r else None

    def cache_member(self, cafe_id: int, member_key: str, member_tag: str,
                     nickname: str, masked_id: str, level_name: str, resolved: bool):
        self.conn.execute(
            """INSERT INTO member_cache
               (cafe_id, member_key, member_tag, nickname, masked_id, level_name, cached_at, resolved)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(cafe_id, member_key) DO UPDATE SET
                 member_tag=excluded.member_tag, nickname=excluded.nickname,
                 masked_id=excluded.masked_id, level_name=excluded.level_name,
                 cached_at=excluded.cached_at, resolved=excluded.resolved""",
            (cafe_id, member_key, member_tag, nickname, masked_id, level_name,
             now_ms(), 1 if resolved else 0),
        )
        self.conn.commit()

    def set_article_tag(self, cafe_id: int, article_id: int,
                        member_tag: str, member_level: str = None):
        """게시글에 멤버태그(+등급) 저장. 기존 값이 없을 때만 등급 갱신."""
        if member_level:
            self.conn.execute(
                "UPDATE articles SET member_tag=?, member_level=COALESCE(NULLIF(member_level,''),?) WHERE cafe_id=? AND article_id=?",
                (member_tag, member_level, cafe_id, article_id),
            )
        else:
            self.conn.execute(
                "UPDATE articles SET member_tag=? WHERE cafe_id=? AND article_id=?",
                (member_tag, cafe_id, article_id),
            )
        self.conn.commit()

    # ── 글관리: 등록 작성자 ────────────────────────────────────────────────

    # ── 작성자 그룹 ────────────────────────────────────────────────────────
    def list_writer_groups(self) -> list:
        return [dict(r) for r in self.conn.execute(
            "SELECT id, name, sort_order FROM writer_group ORDER BY sort_order, id"
        ).fetchall()]

    def add_writer_group(self, name: str) -> int:
        r = self.conn.execute("SELECT COALESCE(MAX(sort_order),0)+1 FROM writer_group").fetchone()
        cur = self.conn.execute(
            "INSERT INTO writer_group (name, sort_order, created_at) VALUES (?,?,?)",
            (name.strip(), r[0], now_ms()),
        )
        self.conn.commit()
        return cur.lastrowid

    def rename_writer_group(self, group_id: int, name: str):
        self.conn.execute("UPDATE writer_group SET name=? WHERE id=?", (name.strip(), group_id))
        self.conn.commit()

    def delete_writer_group(self, group_id: int):
        """그룹 삭제. 소속 작성자는 미분류(group_id=NULL)로 이동(등록 자체는 유지)."""
        self.conn.execute("UPDATE watched_writer SET group_id=NULL WHERE group_id=?", (group_id,))
        self.conn.execute("DELETE FROM writer_group WHERE id=?", (group_id,))
        self.conn.commit()

    def set_writer_group(self, cafe_id: int, member_key: str, group_id: int | None):
        self.conn.execute(
            "UPDATE watched_writer SET group_id=? WHERE cafe_id=? AND member_key=?",
            (group_id, cafe_id, member_key),
        )
        self.conn.commit()

    def add_watched_writer(self, cafe_id: int, member_key: str, naver_id: str,
                           nickname: str, masked_id: str, member_tag: str,
                           level_name: str, memo: str = "", group_id: int | None = None):
        self.conn.execute(
            """INSERT INTO watched_writer
               (cafe_id, member_key, naver_id, nickname, masked_id, member_tag,
                level_name, memo, registered_at, group_id)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(cafe_id, member_key) DO UPDATE SET
                 nickname=excluded.nickname, masked_id=excluded.masked_id,
                 member_tag=excluded.member_tag, level_name=excluded.level_name,
                 group_id=COALESCE(excluded.group_id, watched_writer.group_id)""",
            (cafe_id, member_key, naver_id, nickname, masked_id, member_tag,
             level_name, memo, now_ms(), group_id),
        )
        self.conn.commit()

    def remove_watched_writer(self, cafe_id: int, member_key: str):
        self.conn.execute(
            "DELETE FROM watched_writer WHERE cafe_id=? AND member_key=?",
            (cafe_id, member_key),
        )
        self.conn.commit()

    def get_watched_writers(self) -> list:
        """등록 작성자 목록 + 각자의 게시글 수 통계."""
        return [dict(r) for r in self.conn.execute(
            """SELECT w.*,
                   (SELECT COUNT(*) FROM articles a
                    WHERE a.cafe_id=w.cafe_id AND a.member_key=w.member_key) AS article_count,
                   (SELECT COUNT(*) FROM articles a
                    WHERE a.cafe_id=w.cafe_id AND a.member_key=w.member_key AND a.title_changed=1) AS changed_count,
                   (SELECT COUNT(*) FROM articles a
                    WHERE a.cafe_id=w.cafe_id AND a.member_key=w.member_key AND a.status='deleted') AS deleted_count
               FROM watched_writer w ORDER BY w.registered_at DESC"""
        ).fetchall()]

    def get_writer_articles(self, cafe_id: int, member_key: str,
                            limit: int = 200, offset: int = 0) -> list:
        """등록 작성자의 게시글 리스트 (상태 포함, 작성일 최신순)."""
        return [dict(r) for r in self.conn.execute(
            """SELECT a.*, GROUP_CONCAT(DISTINCT h.keyword_text) AS hit_keywords
               FROM articles a
               LEFT JOIN keyword_hits h ON h.cafe_id=a.cafe_id AND h.article_id=a.article_id
               WHERE a.cafe_id=? AND a.member_key=?
               GROUP BY a.cafe_id, a.article_id
               ORDER BY a.write_ts DESC LIMIT ? OFFSET ?""",
            (cafe_id, member_key, limit, offset),
        ).fetchall()]

    def count_writer_articles(self, cafe_id: int, member_key: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM articles WHERE cafe_id=? AND member_key=?",
            (cafe_id, member_key)).fetchone()[0]

    def get_company_articles(self, company_name: str, start_ms: int | None = None,
                             end_ms: int | None = None, limit: int = 500, offset: int = 0) -> list:
        """특정 업체로 매칭된 게시글 리스트 (상태 포함, 작성일 최신순)."""
        where = ["a.company_name=?"]
        params: list = [company_name]
        if start_ms is not None:
            where.append("a.write_ts >= ?"); params.append(start_ms)
        if end_ms is not None:
            where.append("a.write_ts <= ?"); params.append(end_ms)
        return [dict(r) for r in self.conn.execute(
            f"""SELECT a.*, GROUP_CONCAT(DISTINCT h.keyword_text) AS hit_keywords
               FROM articles a
               LEFT JOIN keyword_hits h ON h.cafe_id=a.cafe_id AND h.article_id=a.article_id
               WHERE {' AND '.join(where)}
               GROUP BY a.cafe_id, a.article_id
               ORDER BY a.write_ts DESC LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()]

    # ── API용 조회 ────────────────────────────────────────────────────────

    def get_articles(self, limit: int = 100, offset: int = 0,
                     search: str = "", status_filter: str = "",
                     cafe_id: int | None = None, menu_id: int | None = None) -> list:
        where, params = [], []
        if search:
            where.append("(a.title LIKE ? OR a.current_title LIKE ? OR a.writer_nickname LIKE ?)")
            params += [f"%{search}%", f"%{search}%", f"%{search}%"]
        if status_filter == "active":
            where.append("a.status='active'")
        elif status_filter == "deleted":
            where.append("a.status='deleted'")
        if cafe_id:
            where.append("a.cafe_id=?"); params.append(cafe_id)
        if menu_id is not None:
            where.append("a.menu_id=?"); params.append(menu_id)
        sql = """SELECT a.*, GROUP_CONCAT(DISTINCT h.keyword_text) AS hit_keywords
                 FROM articles a
                 LEFT JOIN keyword_hits h ON h.cafe_id=a.cafe_id AND h.article_id=a.article_id
              """ + (" WHERE " + " AND ".join(where) if where else "")
        sql += " GROUP BY a.cafe_id, a.article_id ORDER BY a.write_ts DESC LIMIT ? OFFSET ?"
        return [dict(r) for r in self.conn.execute(sql, params + [limit, offset]).fetchall()]

    def get_boards(self, cafe_id: int | None = None) -> list:
        """수집된 게시판 목록 (cafe_id, menu_id, menu_name, 글수)."""
        where, params = ["menu_name IS NOT NULL AND menu_name != ''"], []
        if cafe_id:
            where.append("cafe_id=?"); params.append(cafe_id)
        return [dict(r) for r in self.conn.execute(
            f"""SELECT cafe_id, menu_id, menu_name, COUNT(*) AS cnt
                FROM articles WHERE {' AND '.join(where)}
                GROUP BY cafe_id, menu_id ORDER BY cafe_id, cnt DESC""",
            params,
        ).fetchall()]

    def iter_export_rows(self, cafe_id: int | None = None,
                         start_ms: int | None = None, end_ms: int | None = None,
                         status: str = "", member_key: str | None = None):
        """엑셀 내보내기용 게시글 행을 커서로 순회 (대량 스트리밍)."""
        where, params = [], []
        if cafe_id:
            where.append("cafe_id=?"); params.append(cafe_id)
        if member_key:
            where.append("member_key=?"); params.append(member_key)
        if start_ms is not None:
            where.append("write_ts >= ?"); params.append(start_ms)
        if end_ms is not None:
            where.append("write_ts <= ?"); params.append(end_ms)
        if status == "active":
            where.append("status='active'")
        elif status == "deleted":
            where.append("status='deleted'")
        elif status == "changed":
            where.append("title_changed=1")
        sql = ("""SELECT cafe_id, article_id, menu_name, write_ts, title, current_title,
                         title_changed, status, deleted_at, writer_nickname, member_key,
                         member_tag, member_level, company_name, cur_read, cur_comment,
                         cur_like, first_seen_at, content_text,
                         delete_type, deleted_by_nick, deleted_by_tag
                  FROM articles"""
               + (" WHERE " + " AND ".join(where) if where else "")
               + " ORDER BY write_ts DESC")
        # 별도 커서로 순회 (fetchall 없이 메모리 절약)
        cur = self.conn.execute(sql, params)
        while True:
            batch = cur.fetchmany(500)
            if not batch:
                break
            for r in batch:
                yield dict(r)

    def get_comments_text(self, cafe_id: int, article_id: int, max_len: int = 30000) -> str:
        """게시글 댓글을 한 셀 텍스트로 합침 ('닉네임: 내용' 줄바꿈 구분).
        재방문마다 phase가 달라 같은 댓글이 중복 저장되므로 comment_id별 최신 1개만."""
        rows = self.conn.execute(
            """SELECT writer_nickname, content, is_deleted, MAX(crawled_at) AS latest
               FROM comments
               WHERE cafe_id=? AND article_id=?
               GROUP BY comment_id
               ORDER BY comment_id""",
            (cafe_id, article_id),
        ).fetchall()
        parts = []
        for r in rows:
            if r["is_deleted"]:
                continue
            nick = (r["writer_nickname"] or "").strip()
            content = (r["content"] or "").replace("\n", " ").strip()
            if content:
                parts.append(f"{nick}: {content}")
        text = "\n".join(parts)
        return text[:max_len]

    def get_flagged(self, flag_type: str = "all", limit: int = 100, offset: int = 0,
                    search: str = "", cafe_id: int | None = None) -> list:
        """이상 감지 게시글: title_changed | deleted | keyword | duplicate | all.
        스탭삭제(delete_type='staff')된 글은 이미 관리자가 조치한 것이므로 우측 패널에서 제외.
        search: 제목/현재제목/작성자닉/멤버태그/네이버ID/본문에서 검색."""
        # is_flagged(생성컬럼)=1 을 항상 걸어 idx_is_flagged 를 태운다(전체 스캔 방지).
        where = ["a.is_flagged=1", "COALESCE(a.delete_type,'') != 'staff'"]  # 스탭삭제 숨김
        params: list = []
        if flag_type == "title_changed":
            where.append("a.title_changed=1")
        elif flag_type == "deleted":
            where.append("a.status='deleted'")
        elif flag_type == "keyword":
            where.append("a.has_keyword_hit=1")
        elif flag_type == "duplicate":
            where.append("a.is_duplicate=1")
        if cafe_id:
            where.append("a.cafe_id=?"); params.append(cafe_id)
        if search:
            like = f"%{search}%"
            where.append("""(a.title LIKE ? OR a.current_title LIKE ? OR a.writer_nickname LIKE ?
                             OR a.member_tag LIKE ? OR a.naver_id LIKE ? OR a.content_text LIKE ?)""")
            params += [like, like, like, like, like, like]

        # 중복(도배)글은 같은 그룹(작성자+본문)에서 '가장 최근 1개'(대표=dup_rep)만 노출.
        # 대표 여부는 _mark_duplicates/backfill_duplicates 에서 미리 계산해 둠(조회 시 상관
        # 서브쿼리를 돌리면 중복이 많을 때 O(n^2)로 멈추므로 컬럼으로 사전계산).
        # 검색 중일 때는 특정 글을 찾는 목적이므로 전부 노출.
        if not search:
            where.append("(a.is_duplicate=0 OR a.dup_rep=1)")

        sql = f"""SELECT a.*,
                        GROUP_CONCAT(DISTINCT h.keyword_text) AS hit_keywords
                  FROM articles a
                  LEFT JOIN keyword_hits h ON h.cafe_id=a.cafe_id AND h.article_id=a.article_id
                  WHERE {' AND '.join(where)}
                  GROUP BY a.cafe_id, a.article_id
                  ORDER BY a.reviewed ASC,
                           COALESCE(a.flag_ts, a.write_ts) DESC
                  LIMIT ? OFFSET ?"""
        rows = [dict(r) for r in self.conn.execute(sql, params + [limit, offset]).fetchall()]
        return rows

    def set_reviewed(self, cafe_id: int, article_id: int, reviewed: bool):
        self.conn.execute(
            "UPDATE articles SET reviewed=?, reviewed_at=? WHERE cafe_id=? AND article_id=?",
            (1 if reviewed else 0, now_ms() if reviewed else None, cafe_id, article_id),
        )
        self.conn.commit()

    # ── 유지보수 (장기 안정성) ────────────────────────────────────────────

    def maintenance(self, purge_html: bool = True,
                    prune_done_days: int = 40, do_vacuum: bool = False,
                    dedupe_comments: bool = True) -> dict:
        """DB 정리. content_html 제거 + 오래된 완료 재방문 스케줄 삭제 + 댓글 중복 제거.
        prune_done_days: 이 일수보다 오래전에 완료된(done=1) 스케줄 삭제 (추적 종료분).
        dedupe_comments: 재방문마다 phase별로 중복 저장된 댓글을 comment_id별 최신 1개만 남김."""
        c = self.conn.execute
        result = {}
        if purge_html:
            cur = c("UPDATE articles SET content_html=NULL WHERE content_html IS NOT NULL AND content_html!=''")
            result["html_cleared"] = cur.rowcount
            self.conn.commit()
        if dedupe_comments:
            # comment_id별 최신(rowid 최대) 하나만 남기고 나머지 phase 중복 삭제
            cur = c("""DELETE FROM comments WHERE rowid NOT IN (
                         SELECT MAX(rowid) FROM comments
                         GROUP BY cafe_id, article_id, comment_id)""")
            result["comment_dupes_pruned"] = cur.rowcount
            self.conn.commit()
        if prune_done_days:
            cutoff = now_ms() - prune_done_days * 86400 * 1000
            cur = c("DELETE FROM revisit_schedule WHERE done=1 AND checked_at < ?", (cutoff,))
            result["schedule_pruned"] = cur.rowcount
            self.conn.commit()
        if do_vacuum:
            self.conn.execute("VACUUM")
            result["vacuum"] = True
        return result

    def db_size_info(self) -> dict:
        c = self.conn.execute
        page_count = c("PRAGMA page_count").fetchone()[0]
        page_size = c("PRAGMA page_size").fetchone()[0]
        return {
            "size_mb": round(page_count * page_size / 1024 / 1024, 1),
            "articles": c("SELECT COUNT(*) FROM articles").fetchone()[0],
            "comments": c("SELECT COUNT(*) FROM comments").fetchone()[0],
            "schedule_done": c("SELECT COUNT(*) FROM revisit_schedule WHERE done=1").fetchone()[0],
            "html_remaining": c("SELECT COUNT(*) FROM articles WHERE content_html IS NOT NULL AND content_html!=''").fetchone()[0],
        }

    def set_reviewed_batch(self, items: list, reviewed: bool):
        """여러 게시글 확인 상태 일괄 변경. items=[{cafe_id, article_id}, ...]"""
        ts = now_ms() if reviewed else None
        self.conn.executemany(
            "UPDATE articles SET reviewed=?, reviewed_at=? WHERE cafe_id=? AND article_id=?",
            [(1 if reviewed else 0, ts, it["cafe_id"], it["article_id"]) for it in items],
        )
        self.conn.commit()
        return len(items)

    def get_article_detail(self, cafe_id: int, article_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM articles WHERE cafe_id=? AND article_id=?",
            (cafe_id, article_id),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["title_history"] = [dict(r) for r in self.conn.execute(
            "SELECT * FROM title_history WHERE cafe_id=? AND article_id=? ORDER BY changed_at",
            (cafe_id, article_id),
        ).fetchall()]
        d["keyword_hits"] = [dict(r) for r in self.conn.execute(
            "SELECT * FROM keyword_hits WHERE cafe_id=? AND article_id=? ORDER BY found_at",
            (cafe_id, article_id),
        ).fetchall()]
        # 재방문마다 phase가 달라 같은 댓글이 중복 저장되므로 comment_id별 최신 1개만.
        # (phase='first'만 보면 신규 크롤 실패 후 재방문에서 잡힌 글의 댓글이 누락됨)
        d["comments"] = [dict(r) for r in self.conn.execute(
            """SELECT *, MAX(crawled_at) AS latest FROM comments
               WHERE cafe_id=? AND article_id=?
               GROUP BY comment_id ORDER BY comment_id""",
            (cafe_id, article_id),
        ).fetchall()]
        return d

    def get_company_member_list(self) -> list:
        """업체 아이디 리스트: 구글시트 연동 아이디별 (아이디·멤버태그·최근닉네임·마지막작성일).
        닉네임은 실제 게시글의 최신 닉네임(없으면 해석 시점 닉네임)."""
        return [dict(r) for r in self.conn.execute(
            """SELECT cm.naver_id, cm.cafe_id, cm.member_tag, cm.company_name,
                      cm.nickname AS resolved_nick, cm.level_name, cm.masked_id,
                      (SELECT a.writer_nickname FROM articles a
                       WHERE a.cafe_id=cm.cafe_id AND a.member_key=cm.member_key
                       ORDER BY a.write_ts DESC LIMIT 1) AS last_nick,
                      (SELECT MAX(a.write_ts) FROM articles a
                       WHERE a.cafe_id=cm.cafe_id AND a.member_key=cm.member_key) AS last_write_ts,
                      (SELECT COUNT(*) FROM articles a
                       WHERE a.cafe_id=cm.cafe_id AND a.member_key=cm.member_key) AS article_count
               FROM company_member cm
               WHERE cm.status='resolved' AND cm.member_key IS NOT NULL AND cm.member_key!=''
               ORDER BY last_write_ts DESC NULLS LAST, cm.company_name"""
        ).fetchall()]

    # ── 서포터즈 분석 ─────────────────────────────────────────────────────

    def create_supporter_group(self, name: str) -> int:
        cur = self.conn.execute(
            "INSERT INTO supporter_group (name, created_at) VALUES (?,?)", (name, now_ms()))
        self.conn.commit()
        return cur.lastrowid

    def delete_supporter_group(self, group_id: int):
        self.conn.execute("DELETE FROM supporter_group WHERE id=?", (group_id,))
        self.conn.execute("DELETE FROM supporter_member WHERE group_id=?", (group_id,))
        self.conn.commit()

    def get_supporter_groups(self) -> list:
        return [dict(r) for r in self.conn.execute(
            """SELECT g.*, (SELECT COUNT(*) FROM supporter_member m WHERE m.group_id=g.id) AS member_count
               FROM supporter_group g ORDER BY g.created_at""").fetchall()]

    def add_supporter_member(self, group_id: int, cafe_id: int, member_key: str,
                             naver_id: str, nickname: str, member_tag: str, masked_id: str):
        self.conn.execute(
            """INSERT INTO supporter_member
               (group_id, cafe_id, member_key, naver_id, nickname, member_tag, masked_id, added_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(group_id, cafe_id, member_key) DO UPDATE SET
                 nickname=excluded.nickname, member_tag=excluded.member_tag""",
            (group_id, cafe_id, member_key, naver_id, nickname, member_tag, masked_id, now_ms()))
        self.conn.commit()

    def remove_supporter_member(self, group_id: int, cafe_id: int, member_key: str):
        self.conn.execute(
            "DELETE FROM supporter_member WHERE group_id=? AND cafe_id=? AND member_key=?",
            (group_id, cafe_id, member_key))
        self.conn.commit()

    def get_supporter_members(self, group_id: int) -> list:
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM supporter_member WHERE group_id=? ORDER BY added_at", (group_id,)).fetchall()]

    def supporter_stats(self, group_id: int, start_ms: int | None, end_ms: int | None,
                        menu_id: int | None = None,
                        min_img: int = 0, min_len: int = 0) -> list:
        """그룹원별 기간 통계: 게시글수·총조회·평균조회·좋아요·댓글수(+게시판필터).
        min_img/min_len>0이면 '조건충족 게시글수'(사진≥min_img AND 글자≥min_len)를 함께 계산.
        (사진수·글자수는 본문 크롤된 글에서만 집계되므로 '최신값 갱신' 필요)"""
        members = self.get_supporter_members(group_id)
        out = []
        mission = (min_img > 0 or min_len > 0)
        for m in members:
            cid, mk = m["cafe_id"], m["member_key"]
            aw, ap = ["cafe_id=?", "member_key=?"], [cid, mk]
            if start_ms is not None: aw.append("write_ts>=?"); ap.append(start_ms)
            if end_ms is not None: aw.append("write_ts<=?"); ap.append(end_ms)
            if menu_id is not None: aw.append("menu_id=?"); ap.append(menu_id)
            qsel = ""
            if mission:
                qsel = (", SUM(CASE WHEN COALESCE(image_count,0)>=? "
                        "AND COALESCE(content_len,0)>=? THEN 1 ELSE 0 END) qcnt")
                ap = [min_img, min_len] + ap  # SELECT의 ? 가 WHERE보다 앞
            art = self.conn.execute(
                f"""SELECT COUNT(*) cnt, COALESCE(SUM(cur_read),0) reads,
                       COALESCE(AVG(cur_read),0) avg_read, COALESCE(SUM(cur_like),0) likes{qsel}
                    FROM articles WHERE {' AND '.join(aw)}""", ap).fetchone()
            # 댓글 수 (이 사람이 단 댓글, 기간=update_ts)
            cw, cp = ["cafe_id=?", "member_key=?"], [cid, mk]
            if start_ms is not None: cw.append("update_ts>=?"); cp.append(start_ms)
            if end_ms is not None: cw.append("update_ts<=?"); cp.append(end_ms)
            cmt = self.conn.execute(
                f"SELECT COUNT(DISTINCT comment_id) FROM comments WHERE {' AND '.join(cw)}", cp).fetchone()[0]
            # 최근 닉네임
            last_nick = self.conn.execute(
                "SELECT writer_nickname FROM articles WHERE cafe_id=? AND member_key=? ORDER BY write_ts DESC LIMIT 1",
                (cid, mk)).fetchone()
            row = {
                "cafe_id": cid, "member_key": mk, "naver_id": m["naver_id"],
                "member_tag": m["member_tag"],
                "nickname": (last_nick[0] if last_nick else m["nickname"]) or m["nickname"],
                "article_count": art["cnt"], "total_read": art["reads"],
                "avg_read": round(art["avg_read"], 1), "total_like": art["likes"],
                "comment_count": cmt,
            }
            if mission:
                row["qualify_count"] = art["qcnt"] or 0
            out.append(row)
        return out

    def supporter_articles(self, group_id: int, start_ms: int | None, end_ms: int | None,
                           menu_id: int | None = None, limit: int = 2000) -> list:
        """그룹원들이 작성한 게시글 목록 (사진수·글자수 포함)."""
        members = self.get_supporter_members(group_id)
        if not members:
            return []
        conds = " OR ".join(["(a.cafe_id=? AND a.member_key=?)"] * len(members))
        params: list = []
        for m in members:
            params += [m["cafe_id"], m["member_key"]]
        extra = []
        if start_ms is not None: extra.append("a.write_ts>=?"); params.append(start_ms)
        if end_ms is not None: extra.append("a.write_ts<=?"); params.append(end_ms)
        if menu_id is not None: extra.append("a.menu_id=?"); params.append(menu_id)
        where = f"({conds})" + ("".join(" AND " + e for e in extra))
        return [dict(r) for r in self.conn.execute(
            f"""SELECT a.cafe_id, a.article_id, a.menu_name, a.write_ts, a.title, a.current_title,
                       a.writer_nickname, a.member_tag, a.status, a.title_changed,
                       a.cur_read, a.cur_comment, a.cur_like, a.image_count, a.content_len,
                       a.body_crawled
                FROM articles a WHERE {where}
                ORDER BY a.write_ts DESC LIMIT ?""", params + [limit]).fetchall()]

    def get_supporter_boards(self, group_id: int) -> list:
        """그룹원들이 글을 쓴 게시판 목록 (필터 드롭다운용)."""
        members = self.get_supporter_members(group_id)
        if not members:
            return []
        conds = " OR ".join(["(cafe_id=? AND member_key=?)"] * len(members))
        params: list = []
        for m in members:
            params += [m["cafe_id"], m["member_key"]]
        return [dict(r) for r in self.conn.execute(
            f"""SELECT menu_id, menu_name, COUNT(*) cnt FROM articles
                WHERE ({conds}) AND menu_name IS NOT NULL AND menu_name!=''
                GROUP BY menu_id ORDER BY cnt DESC""", params).fetchall()]

    # --- 네이버 카페 통계도구(멤버순위) 캐시 ---
    def save_naver_stats(self, cafe_id: int, year_month: str, merged: dict) -> int:
        """fetch_member_stats_all() 결과(dict)를 캐시에 저장."""
        ts = now_ms()
        n = 0
        for uid, d in merged.items():
            self.conn.execute(
                """INSERT OR REPLACE INTO naver_member_stat
                   (cafe_id, year_month, user_id, nickname, level_name, join_date,
                    n_create, n_comment, n_like_sent, n_like_recv, n_view, n_visit, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (cafe_id, year_month, uid, d.get("nickname"), d.get("level_name"),
                 d.get("join_date"), d.get("create"), d.get("comment"),
                 d.get("like_sent"), d.get("like_recv"), d.get("view"), d.get("visit"), ts))
            n += 1
        self.conn.commit()
        return n

    def get_naver_stats(self, cafe_id: int, year_month: str) -> dict:
        """캐시된 네이버 통계를 {user_id: row} 로 반환."""
        rows = self.conn.execute(
            "SELECT * FROM naver_member_stat WHERE cafe_id=? AND year_month=?",
            (cafe_id, year_month)).fetchall()
        return {r["user_id"]: dict(r) for r in rows}

    def naver_stats_age(self, cafe_id: int, year_month: str) -> int | None:
        """캐시 최신 updated_at(ms). 없으면 None."""
        r = self.conn.execute(
            "SELECT MAX(updated_at) FROM naver_member_stat WHERE cafe_id=? AND year_month=?",
            (cafe_id, year_month)).fetchone()
        return r[0] if r and r[0] else None

    def get_company_stats(self, start_ms: int | None = None,
                          end_ms: int | None = None) -> list:
        """업체별 통계. start_ms~end_ms(작성일 write_ts 기준) 범위 필터 가능."""
        where = ["a.company_name IS NOT NULL AND a.company_name != ''"]
        params: list = []
        if start_ms is not None:
            where.append("a.write_ts >= ?")
            params.append(start_ms)
        if end_ms is not None:
            where.append("a.write_ts <= ?")
            params.append(end_ms)
        return self.conn.execute(
            f"""SELECT
                   a.company_name,
                   COUNT(*) AS total,
                   SUM(CASE WHEN a.status='active' THEN 1 ELSE 0 END) AS active_count,
                   SUM(CASE WHEN a.status='deleted' THEN 1 ELSE 0 END) AS deleted_count,
                   SUM(CASE WHEN a.title_changed=1 THEN 1 ELSE 0 END) AS title_changed_count,
                   SUM(CASE WHEN a.has_keyword_hit=1 THEN 1 ELSE 0 END) AS keyword_hit_count,
                   MAX(a.write_ts) AS last_write_ts
               FROM articles a
               WHERE {' AND '.join(where)}
               GROUP BY a.company_name
               ORDER BY total DESC""",
            params,
        ).fetchall()

    def get_keyword_stats(self) -> list:
        return self.conn.execute(
            """SELECT k.id, k.keyword, k.is_active,
                      COUNT(h.id) AS total_hits,
                      SUM(CASE WHEN h.found_at >= ? THEN 1 ELSE 0 END) AS today_hits,
                      MAX(h.found_at) AS last_hit_at
               FROM keywords k
               LEFT JOIN keyword_hits h ON h.keyword_id=k.id
               GROUP BY k.id ORDER BY total_hits DESC""",
            (now_ms() - 86400000,),
        ).fetchall()

    def get_stats(self) -> dict:
        c = self.conn.execute
        return {
            "total":        c("SELECT COUNT(*) FROM articles").fetchone()[0],
            "active":       c("SELECT COUNT(*) FROM articles WHERE status='active'").fetchone()[0],
            # 우측 삭제 탭 배지용: 스탭삭제 제외한 삭제(자삭+미정)
            "deleted":      c("SELECT COUNT(*) FROM articles WHERE status='deleted' AND COALESCE(delete_type,'')!='staff'").fetchone()[0],
            "deleted_staff":c("SELECT COUNT(*) FROM articles WHERE delete_type='staff'").fetchone()[0],
            "title_changed":c("SELECT COUNT(*) FROM articles WHERE title_changed=1").fetchone()[0],
            "keyword_hit":  c("SELECT COUNT(*) FROM articles WHERE has_keyword_hit=1").fetchone()[0],
            # 우측 중복 탭 배지: 실제 표시되는 대표(계정당 최신 1건)만 카운트
            "duplicate":    c("SELECT COUNT(*) FROM articles WHERE is_duplicate=1 AND dup_rep=1").fetchone()[0],
            "with_company": c("SELECT COUNT(*) FROM articles WHERE company_name IS NOT NULL AND company_name!=''").fetchone()[0],
            "keywords":     c("SELECT COUNT(*) FROM keywords WHERE is_active=1").fetchone()[0],
            "companies":    c("SELECT COUNT(DISTINCT company_name) FROM company_master").fetchone()[0],
        }

    # ── 메타 ──────────────────────────────────────────────────────────────

    def get_meta(self, key: str) -> str | None:
        r = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r[0] if r else None

    def set_meta(self, key: str, value: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)", (key, value)
        )
        self.conn.commit()
