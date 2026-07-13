"""실시간 크롤링 엔진 — 다중 카페 라운드로빈."""
from __future__ import annotations

import itertools
import time
from dataclasses import dataclass
from pathlib import Path

from . import cafe_api
from .db import Database, now_ms
from .keyword_monitor import scan_article
from .session import SessionManager


@dataclass
class CafeTarget:
    cafe_url: str
    cafe_id: int
    menu_id: int
    name: str


class Watcher:
    def __init__(
        self,
        db: Database,
        client,
        targets: list[CafeTarget],
        *,
        per_page: int = 50,
        min_gap_s: float = 1.5,
        on_event=None,
        log=print,
        sheet_cfg: dict | None = None,
    ):
        self.db = db
        self.client = client
        self.targets = targets
        self.per_page = per_page
        self.min_gap = min_gap_s
        self.on_event = on_event
        self.log = log
        self.sheet_cfg = sheet_cfg or {}   # {spreadsheet_id, gid}

        self.session_ok = False
        self._last_request = 0.0
        self._errors = 0
        self._last_session_check = 0.0
        self._session_check_gap = 120.0
        # 시트 동기화 / 회원해석 주기 상태
        self._last_sheet_sync = 0.0
        self._sheet_sync_gap = 1800.0   # 30분마다 시트 재동기화

    # ── 속도 제한 ──────────────────────────────────────────────────────────

    def _throttle(self):
        wait = self.min_gap - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    # ── 세션 체크 ──────────────────────────────────────────────────────────

    def check_session(self, force: bool = False):
        if not force and (time.monotonic() - self._last_session_check) < self._session_check_gap:
            return
        self._last_session_check = time.monotonic()
        ok = cafe_api.check_login(self.client)
        if ok != self.session_ok:
            self.session_ok = ok
            self.log(f"  {'✓ 세션 정상' if ok else '⚠ 세션 만료'}")
            self._emit("session", {"ok": ok})

    # ── 게시글 목록 폴링 ───────────────────────────────────────────────────

    def poll(self, target: CafeTarget):
        self._throttle()
        arts = cafe_api.fetch_article_list(
            target.cafe_id, menu_id=target.menu_id,
            per_page=self.per_page, client=self.client,
        )
        live = [a for a in arts if not (a.blinded or a.is_notice)]
        new_count = 0
        for a in live:
            is_new = self.db.upsert_article(a, session_ok=self.session_ok)
            if is_new:
                new_count += 1
                self._crawl_new(a, target)
        self.db.update_counts_bulk(live)
        return len(arts), new_count

    # ── 신규 게시글 처리 ───────────────────────────────────────────────────

    def _crawl_new(self, a, target: CafeTarget):
        try:
            self._throttle()
            body = cafe_api.fetch_article_body(
                a.cafe_id, a.article_id, menu_id=a.menu_id, client=self.client
            )
            self.db.save_body(body)

            self._throttle()
            comments = cafe_api.fetch_comments(a.cafe_id, a.article_id, client=self.client)
            self.db.save_comments(a.cafe_id, a.article_id, comments, phase="first")

            scan_article(
                self.db, a.cafe_id, a.article_id,
                body.title, body.content_text, comments, phase="first",
            )

            # 업체/등급 매칭 — 작성자 member_key로 company_member 조회
            company, member_tag, level = self._match_member(a)

            self.log(
                f"  + NEW [{target.name}][{a.article_id}] {a.title[:30]}"
                + (f" [{company}]" if company else "")
                + (f" <{level}>" if level else "")
                + f"  조회{a.read_count} 댓글{len(comments)}"
            )
            has_kw = self.db.conn.execute(
                "SELECT has_keyword_hit FROM articles WHERE cafe_id=? AND article_id=?",
                (a.cafe_id, a.article_id),
            ).fetchone()
            self._emit("new_article", {
                "cafe_id": a.cafe_id,
                "cafe_name": target.name,
                "article_id": a.article_id,
                "menu_name": a.menu_name,
                "title": a.title,
                "writer_nickname": a.writer_nickname,
                "company_name": company,
                "member_tag": member_tag,
                "member_level": level,
                "read_count": a.read_count,
                "comment_count": len(comments),
                "like_count": a.like_count,
                "write_ts": a.write_ts,
                "url": a.url,
                "has_keyword_hit": has_kw["has_keyword_hit"] if has_kw else 0,
            })
        except Exception as e:
            self.log(f"  ! 신규 크롤 실패 [{a.article_id}]: {e}")

    def _match_member(self, a) -> tuple[str | None, str | None, str | None]:
        """작성자 member_key로 업체/멤버태그/등급 매칭 후 게시글에 저장.
        업체가 아니어도 멤버태그는 항상 확보(삭제 후 작성자 추적용).
        Returns (company_name, member_tag, level_name)."""
        company = tag = level = None
        # 1) 업체 매칭 우선 (구글시트 기반)
        info = self.db.get_company_by_member_key(a.cafe_id, a.member_key)
        if info:
            company = info.get("company_name")
            tag = info.get("member_tag")
            level = info.get("level_name")
            # 업체글은 촘촘한 재방문 스케줄로 업그레이드 (수정·삭제 놓치지 않게)
            self.db.upgrade_to_company_schedule(a.cafe_id, a.article_id)
        # 2) 멤버태그가 아직 없으면 캐시 → 회원관리 조회
        if not tag and a.member_key:
            tag, level2 = self._resolve_tag(a.cafe_id, a.member_key, a.writer_nickname)
            level = level or level2
        self.db.update_member_info(a.cafe_id, a.article_id, tag, level, company)
        return company, tag, level

    def _resolve_tag(self, cafe_id: int, member_key: str, nickname: str):
        """member_key → (member_tag, level_name). 캐시 우선, 없으면 회원관리 조회."""
        cached = self.db.get_cached_member(cafe_id, member_key)
        if cached:
            return cached.get("member_tag"), cached.get("level_name")
        # 세션 없으면 조회 불가
        if not self.session_ok:
            return None, None
        try:
            self._throttle()
            m = cafe_api.resolve_member_by_key(cafe_id, member_key, nickname, self.client)
        except Exception:
            m = None
        if m:
            self.db.cache_member(cafe_id, member_key, m.member_tag, m.nickname,
                                 m.masked_id, m.level_name, resolved=True)
            return m.member_tag, m.level_name
        # 조회 실패도 캐시(재시도 방지, 태그 없음)
        self.db.cache_member(cafe_id, member_key, "", nickname, "", "", resolved=False)
        return None, None

    # ── 재방문 처리 ────────────────────────────────────────────────────────

    def process_revisits(self):
        due = self.db.due_revisits()
        for row in due:
            cafe_id = row["cafe_id"]
            article_id = row["article_id"]
            phase = row["phase"]
            try:
                self._throttle()
                try:
                    body = cafe_api.fetch_article_body(
                        cafe_id, article_id,
                        menu_id=row["menu_id"] or 0,
                        client=self.client,
                    )
                except cafe_api.ArticleGoneError:
                    self.db.mark_deleted(cafe_id, article_id)
                    self.log(f"  x DELETED [{article_id}] ({phase})")
                    self._emit("deleted", {"cafe_id": cafe_id, "article_id": article_id, "phase": phase})
                    continue

                # 신규 크롤 때 본문 확보 실패했으면(body_crawled=0) 이번에 저장
                self.db.save_body(body)

                changed = self.db.check_title_change(cafe_id, article_id, body.title, phase)
                if changed:
                    old = row["current_title"]
                    self.log(f"  ~ TITLE [{article_id}] ({phase}) '{old}' → '{body.title}'")
                    self._emit("title_changed", {
                        "cafe_id": cafe_id, "article_id": article_id, "phase": phase,
                        "old_title": old, "new_title": body.title,
                    })

                comments = []
                try:
                    self._throttle()
                    comments = cafe_api.fetch_comments(cafe_id, article_id, client=self.client)
                    self.db.save_comments(cafe_id, article_id, comments, phase=phase)
                except Exception:
                    pass

                scan_article(
                    self.db, cafe_id, article_id,
                    body.title, body.content_text, comments, phase=phase,
                )
                self.db.complete_revisit(cafe_id, article_id, phase)

            except Exception as e:
                self.log(f"  ! 재방문 실패 [{article_id}] ({phase}): {e}")

    # ── 이벤트 ────────────────────────────────────────────────────────────

    def _emit(self, kind: str, payload: dict):
        if self.on_event:
            try:
                self.on_event(kind, payload)
            except Exception as e:
                self.log(f"  ! 이벤트 발행 실패: {e}")

    def _on_error(self, target_name: str, err):
        self._errors += 1
        backoff = min(60.0, self.min_gap * (2 ** self._errors))
        self.log(f"  … [{target_name}] 오류 {self._errors}회 → {backoff:.1f}s 백오프: {err}")
        time.sleep(backoff)
        if self._errors >= 3:
            self.check_session(force=True)

    def _on_success(self):
        self._errors = 0

    # ── 메인 루프 (라운드로빈) ─────────────────────────────────────────────

    def run(self, tick_s: float = 1.5):
        names = ", ".join(f"{t.name}(menu:{t.menu_id})" for t in self.targets)
        self.log(f"Watcher 시작 — 카페: {names}  tick={tick_s}s")
        self.check_session(force=True)

        cycle = itertools.cycle(self.targets)
        i = 0
        while True:
            target = next(cycle)
            try:
                total, new = self.poll(target)
                self._on_success()
                if new:
                    self.log(f"■ [{target.name}] 신규 {new}건 (확인 {total}건)")
            except Exception as e:
                self._on_error(target.name, e)

            i += 1
            if i % max(len(self.targets), 1) == 0:
                self.process_revisits()
                self.check_session()
                self._maybe_sync_sheet()
                self._resolve_some()

            time.sleep(tick_s)

    # ── 시트 동기화 / 회원 해석 ────────────────────────────────────────────

    def _maybe_sync_sheet(self):
        """주기적으로 구글시트를 재동기화 (신규 업체 ID 반영)."""
        if not self.sheet_cfg.get("spreadsheet_id"):
            return
        if (time.monotonic() - self._last_sheet_sync) < self._sheet_sync_gap:
            return
        self._last_sheet_sync = time.monotonic()
        try:
            from .company_matcher import sync_sheet
            cafe_ids = [t.cafe_id for t in self.targets]
            res = sync_sheet(self.db, self.sheet_cfg["spreadsheet_id"],
                             self.sheet_cfg.get("gid", 0), cafe_ids, log=self.log)
            self._emit("sheet_sync", res)
        except Exception as e:
            self.log(f"  ! 시트 동기화 실패: {e}")

    def _resolve_some(self):
        """매 사이클 pending 회원을 소량씩 해석 (백그라운드 점진 처리)."""
        if not self.session_ok:
            return
        try:
            from .company_matcher import resolve_pending
            done = resolve_pending(self.db, self.client, batch=5, min_gap_s=0.3, log=self.log)
            if done:
                self._emit("resolve_progress", self.db.resolution_stats())
        except Exception as e:
            self.log(f"  ! 회원 해석 실패: {e}")


def build_watcher(db: Database, session_dir: Path, account_id: str | None,
                  targets: list[CafeTarget], **kwargs) -> Watcher:
    sm = SessionManager(session_dir)
    cookies = None
    if account_id and sm.verify(account_id).ok:
        cookies = sm.load_cookies(account_id)
    client = cafe_api.make_client(cookies)
    return Watcher(db, client, targets, **kwargs)
