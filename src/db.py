"""SQLite persistence — 카페 게시글 감시 시스템."""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

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
                          ("deleted_at", "INTEGER"), ("flag_ts", "INTEGER")):
            if col not in cols:
                self.conn.execute(f"ALTER TABLE articles ADD COLUMN {col} {decl}")
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
        is_new = cur.rowcount > 0
        if is_new:
            self._schedule_revisits(a.cafe_id, a.article_id, ts)
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
        self.conn.execute(
            """UPDATE articles SET content_text=?, content_html=?, body_crawled=1,
               menu_id=COALESCE(NULLIF(menu_id,0), ?)
               WHERE cafe_id=? AND article_id=?""",
            (body.content_text, body.content_html, body.menu_id,
             body.cafe_id, body.article_id),
        )
        self.conn.commit()

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
        # 새 이상 발생 → flag_ts 갱신, reviewed 리셋 (다시 미확인으로 상단 배치)
        self.conn.execute(
            """UPDATE articles SET status='deleted', deleted_at=?, flag_ts=?, reviewed=0
               WHERE cafe_id=? AND article_id=?""",
            (ts, ts, cafe_id, article_id),
        )
        self.conn.execute(
            "UPDATE revisit_schedule SET done=1 WHERE cafe_id=? AND article_id=?",
            (cafe_id, article_id),
        )
        self.conn.commit()

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

    def add_watched_writer(self, cafe_id: int, member_key: str, naver_id: str,
                           nickname: str, masked_id: str, member_tag: str,
                           level_name: str, memo: str = ""):
        self.conn.execute(
            """INSERT INTO watched_writer
               (cafe_id, member_key, naver_id, nickname, masked_id, member_tag,
                level_name, memo, registered_at)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(cafe_id, member_key) DO UPDATE SET
                 nickname=excluded.nickname, masked_id=excluded.masked_id,
                 member_tag=excluded.member_tag, level_name=excluded.level_name""",
            (cafe_id, member_key, naver_id, nickname, masked_id, member_tag,
             level_name, memo, now_ms()),
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

    def get_flagged(self, flag_type: str = "all", limit: int = 100, offset: int = 0) -> list:
        """이상 감지 게시글: title_changed | deleted | keyword | all."""
        where = []
        if flag_type == "title_changed":
            where.append("a.title_changed=1")
        elif flag_type == "deleted":
            where.append("a.status='deleted'")
        elif flag_type == "keyword":
            where.append("a.has_keyword_hit=1")
        else:
            where.append("(a.title_changed=1 OR a.status='deleted' OR a.has_keyword_hit=1)")

        sql = f"""SELECT a.*,
                        GROUP_CONCAT(DISTINCT h.keyword_text) AS hit_keywords
                  FROM articles a
                  LEFT JOIN keyword_hits h ON h.cafe_id=a.cafe_id AND h.article_id=a.article_id
                  WHERE {' AND '.join(where)}
                  GROUP BY a.cafe_id, a.article_id
                  ORDER BY a.reviewed ASC,
                           COALESCE(a.flag_ts, a.write_ts) DESC
                  LIMIT ? OFFSET ?"""
        rows = [dict(r) for r in self.conn.execute(sql, [limit, offset]).fetchall()]
        return rows

    def set_reviewed(self, cafe_id: int, article_id: int, reviewed: bool):
        self.conn.execute(
            "UPDATE articles SET reviewed=?, reviewed_at=? WHERE cafe_id=? AND article_id=?",
            (1 if reviewed else 0, now_ms() if reviewed else None, cafe_id, article_id),
        )
        self.conn.commit()

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
        d["comments"] = [dict(r) for r in self.conn.execute(
            """SELECT * FROM comments WHERE cafe_id=? AND article_id=? AND phase='first'
               ORDER BY comment_id""",
            (cafe_id, article_id),
        ).fetchall()]
        return d

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
            "deleted":      c("SELECT COUNT(*) FROM articles WHERE status='deleted'").fetchone()[0],
            "title_changed":c("SELECT COUNT(*) FROM articles WHERE title_changed=1").fetchone()[0],
            "keyword_hit":  c("SELECT COUNT(*) FROM articles WHERE has_keyword_hit=1").fetchone()[0],
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
