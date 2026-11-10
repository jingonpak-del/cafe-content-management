"""실시간 크롤링 엔진 — 다중 카페 라운드로빈."""
from __future__ import annotations

import itertools
import queue
import threading
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
        session_mgr=None,
        account: str | None = None,
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
        # 세션 재저장 — 네이버가 굴려준 쿠키를 파일로 되쓴다.
        # 이게 없으면 갱신분이 메모리에만 남아, 재시작하는 순간 처음 캡처한 낡은 쿠키로
        # 되돌아간다. 실제로 그 상태였다(snowgreent 세션이 명목 만료 1.8일 경과).
        self.session_mgr = session_mgr
        self.account = account
        self._last_persist = 0.0
        self._persist_gap = 600.0
        self._saved_fp = None
        # 시트 동기화 / 회원해석 주기 상태
        self._last_sheet_sync = 0.0
        self._sheet_sync_gap = 1800.0   # 30분마다 시트 재동기화
        # DB 유지보수 주기 (하루 1회)
        self._last_maintenance = time.monotonic()   # 시작 직후엔 스킵
        self._maintenance_gap = 86400.0
        # 과거글 수집 주기 (30분마다 카페당 1페이지=50건 → 하루 ~2,400건)
        self._last_past = 0.0
        self._past_gap = 1800.0
        self._past_daily_limit = 2000    # 카페당 하루 상한
        # 스탭 삭제글 동기화 주기 (30분)
        self._last_removed = 0.0
        self._removed_gap = 1800.0
        # 목록 순회(최근 SWEEP_DAYS일 글 제목·댓글수 대조) 상태 — 카페별 커서
        self._sweep_thread = None
        self._sweep_missing: queue.Queue = queue.Queue()
        # not_member 재해석 주기 (6시간)
        self._last_notmember_retry = 0.0
        self._notmember_gap = 21600.0

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
        if ok:
            # 로그인이 살아 있을 때만 저장한다. 죽은 뒤 저장하면 쓸모없는 쿠키로 덮어쓴다.
            self.persist_session()

    def persist_session(self):
        """네이버가 갱신해 준 쿠키를 세션 파일에 되쓴다(10분 간격, 변화 있을 때만)."""
        if not self.session_mgr or not self.account:
            return
        if (time.monotonic() - self._last_persist) < self._persist_gap:
            return
        self._last_persist = time.monotonic()
        try:
            fp = self.session_mgr.persist(self.account, self.client, prev=self._saved_fp)
        except Exception as e:
            self.log(f"  ! 세션 저장 실패: {e}")
            return
        if fp is None:
            return          # 갱신 없음 또는 로그인 쿠키 없음 — 정상 경로
        first = self._saved_fp is None
        self._saved_fp = fp
        v = self.session_mgr.verify(self.account)
        self.log(f"  ⟳ 세션 {'저장' if first else '갱신 저장'} — {v.expiry_text}")

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
        if self._studio_blocked():
            return None, None
        try:
            self._throttle()
            m = cafe_api.resolve_member_by_key(cafe_id, member_key, nickname, self.client)
        except cafe_api.StudioAuthRequired:
            return None, None          # 본인인증 전 — 실패로 캐시하지 않음(인증 후 다시 조회)
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

                # 본문 수정 감지(저장 전에 이전 본문과 비교) — 작성 후 홍보글로 바꾸는 수법
                if self.db.check_body_change(cafe_id, article_id, body.content_text, phase):
                    self.log(f"  ✎ BODY [{article_id}] ({phase}) 본문 수정됨")
                    self._emit("body_changed", {"cafe_id": cafe_id, "article_id": article_id, "phase": phase})
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
                    # 기존 댓글 내용 수정 감지(저장 전 비교) — 정상 댓글을 홍보 문구로 고치는 수법
                    edits = self.db.check_comment_edits(cafe_id, article_id, comments, phase)
                    if edits:
                        self.log(f"  ✎ COMMENT-EDIT [{article_id}] ({phase}) 댓글 {len(edits)}개 내용 수정됨")
                        self._emit("comment_edited", {"cafe_id": cafe_id, "article_id": article_id,
                                                      "phase": phase, "count": len(edits)})
                    self.db.save_comments(cafe_id, article_id, comments, phase=phase)
                except Exception:
                    pass

                # 최신 조회수·댓글수 반영 + 댓글 변동 감지 (24h 내 추가 댓글 등)
                delta = self.db.update_revisit_counts(
                    cafe_id, article_id, body.read_count, body.comment_count)
                if delta:
                    self.log(f"  💬 COMMENT [{article_id}] ({phase}) 댓글 {'+' if delta>0 else ''}{delta}건 변동")
                    self._emit("comment_changed", {
                        "cafe_id": cafe_id, "article_id": article_id,
                        "phase": phase, "delta": delta, "count": body.comment_count,
                    })

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

    def _reopen_db(self, err):
        """11-10 19:28~11-11 06:10 수집이 11시간 멈춘 원인: DB 연결 하나가 'database is locked' 에 걸려 풀리지 않았다.
        (다른 연결로는 쓰기가 되는데 이 연결만 계속 실패 — 옛 읽기 스냅샷이 남은 경우) 연결을 새로 연다."""
        import sqlite3 as _sq
        from pathlib import Path as _P
        try:
            with open(_P(self.db.path).parent / "watcher_error.log", "a", encoding="utf-8") as f:
                f.write(time.strftime("%m-%d %H:%M:%S") + f" {type(err).__name__}: {err}" + chr(10))
        except Exception:
            pass
        try:
            self.db.conn.close()
        except Exception:
            pass
        conn = _sq.connect(str(self.db.path), check_same_thread=False, timeout=15)
        conn.row_factory = _sq.Row
        conn.execute("PRAGMA synchronous=NORMAL")
        self.db.conn = conn
        self.log("  ↻ 수집 DB 연결을 새로 열었습니다")

    def _on_error(self, target_name: str, err):
        self._errors += 1
        backoff = min(60.0, self.min_gap * (2 ** self._errors))
        import sqlite3 as _sq
        if isinstance(err, _sq.Error):
            backoff = min(backoff, 5.0)       # DB 오류는 오래 쉬지 않고 연결을 새로 열어 바로 재시도
            try:
                self._reopen_db(err)
            except Exception as e2:
                self.log(f"  ! DB 재연결 실패: {e2}")
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
        self._start_sweep()
        from .scam_guard import ScamGuard
        self._scam_guard = ScamGuard(self)
        self._scam_guard.start()

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
                self._maybe_retry_notmember()
                self._drain_sweep_missing()
                self._maybe_maintenance()
                self._maybe_backfill_past()
                self._maybe_sync_removed()

            time.sleep(tick_s)

    # ── 스탭 삭제글 동기화 (자삭/스탭삭제 판별) ──────────────────────────────

    def _maybe_sync_removed(self):
        """30분마다 스탭 삭제 목록을 크롤해 매칭글을 staff로 표시하고,
        유예 지난 미정 삭제글은 self(자삭)로 확정."""
        if not self.session_ok:
            return
        if (time.monotonic() - self._last_removed) < self._removed_gap:
            return
        self._last_removed = time.monotonic()
        for target in self.targets:
            cid = target.cafe_id
            staff_found = 0
            try:
                # 최근 여러 페이지 훑기 (신규 삭제는 앞페이지에 나타남)
                for page in range(1, 11):
                    self._throttle()
                    removed = cafe_api.fetch_removed_articles(cid, page, client=self.client)
                    if not removed:
                        break
                    page_new = 0
                    for ra in removed:
                        if self.db.apply_staff_deletion(ra):
                            page_new += 1
                    staff_found += page_new
                    # 이 페이지에서 우리 DB와 매칭된 게 하나도 없으면 더 과거는 스킵
                    if page_new == 0 and page >= 3:
                        break
            except Exception as e:
                self.log(f"  ! 스탭삭제 동기화 실패 [{target.name}]: {e}")
            if staff_found:
                self.log(f"  🛡️ 스탭삭제 반영 [{target.name}]: {staff_found}건")
        # 유예 지난 미정 삭제 → 자삭 확정
        try:
            n = self.db.finalize_self_deletions(grace_hours=6)
            if n:
                self.log(f"  👤 자삭 확정: {n}건")
        except Exception:
            pass

    # ── 과거글 수집 (목록 역순 → 번호 역순) ─────────────────────────────────

    def _past_today_count(self, cid: int) -> tuple[str, int]:
        """오늘 날짜와 그날 수집량을 meta에서 읽음 (YYYYMMDD:count)."""
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        raw = self.db.get_meta(f"past_{cid}_today") or ""
        if raw.startswith(today + ":"):
            try:
                return today, int(raw.split(":")[1])
            except Exception:
                return today, 0
        return today, 0

    def _maybe_backfill_past(self):
        """30분마다 카페별로 과거글을 조금씩 수집 (하루 상한 내에서)."""
        if (time.monotonic() - self._last_past) < self._past_gap:
            return
        self._last_past = time.monotonic()
        for target in self.targets:
            try:
                self._backfill_past_one(target)
            except Exception as e:
                self.log(f"  ! 과거 수집 실패 [{target.name}]: {e}")

    def _backfill_past_one(self, target):
        cid = target.cafe_id
        phase = self.db.get_meta(f"past_{cid}_phase") or "list"
        if phase == "done":
            return
        today, cnt = self._past_today_count(cid)
        if cnt >= self._past_daily_limit:
            return   # 오늘 상한 도달

        if phase == "list":
            page = int(self.db.get_meta(f"past_{cid}_page") or 2)  # page1은 실시간이 담당
            self._throttle()
            arts = cafe_api._fetch_boardlist(cid, 0, 50, page, client=self.client)
            if not arts:
                self._switch_to_number_phase(cid)
                return
            page_min = min(a.article_id for a in arts)
            # 롤백 감지: 이 페이지 최소 article_id가 이미 우리가 가진 최대 근처면 목록 한계
            db_max = self.db.conn.execute(
                "SELECT MAX(article_id) FROM articles WHERE cafe_id=?", (cid,)
            ).fetchone()[0] or 0
            if page_min >= db_max - 100:
                self._switch_to_number_phase(cid)
                return
            new = 0
            live = [a for a in arts if not (a.blinded or a.is_notice)]
            for a in live:
                if self.db.upsert_article(a):   # 신규면 재방문 스케줄 자동 → body/댓글 점진
                    new += 1
            self.db.update_counts_bulk(live)
            self.db.set_meta(f"past_{cid}_page", str(page + 1))
            self.db.set_meta(f"past_{cid}_today", f"{today}:{cnt + new}")
            if new:
                self.log(f"  📚 과거수집 [{target.name}] page{page}: 신규 {new}건 (오늘 {cnt+new})")

        elif phase == "number":
            num = int(self.db.get_meta(f"past_{cid}_num") or 0)
            if num <= 1:
                self.db.set_meta(f"past_{cid}_phase", "done")
                self.log(f"  ✅ 과거수집 완료 [{target.name}]")
                return
            got = 0
            # 한 배치에 최대 30개 번호 시도 (없는 번호/삭제 스킵)
            tried = 0
            while tried < 30 and (cnt + got) < self._past_daily_limit and num > 1:
                tried += 1
                num -= 1
                if self.db.conn.execute(
                    "SELECT 1 FROM articles WHERE cafe_id=? AND article_id=?", (cid, num)
                ).fetchone():
                    continue   # 이미 있음
                self._throttle()
                try:
                    body = cafe_api.fetch_article_body(cid, num, menu_id=0, client=self.client)
                    self.db.upsert_body_as_article(cid, num, body)
                    got += 1
                except cafe_api.ArticleGoneError:
                    continue
                except Exception:
                    continue
            self.db.set_meta(f"past_{cid}_num", str(num))
            self.db.set_meta(f"past_{cid}_today", f"{today}:{cnt + got}")
            if got:
                self.log(f"  📚 과거수집(번호) [{target.name}] ~{num}: 신규 {got}건")

    def _switch_to_number_phase(self, cid: int):
        """목록 단계 종료 → 번호 역순 단계로. 시작 번호 = 현재 DB 최소 article_id - 1."""
        db_min = self.db.conn.execute(
            "SELECT MIN(article_id) FROM articles WHERE cafe_id=?", (cid,)
        ).fetchone()[0] or 1
        self.db.set_meta(f"past_{cid}_phase", "number")
        self.db.set_meta(f"past_{cid}_num", str(db_min))
        self.log(f"  📚 과거수집: 목록 한계 도달 → 번호 역순 전환 (from {db_min})")

    def _maybe_maintenance(self):
        """하루 1회 DB 정리 (content_html 제거 + 오래된 완료 스케줄 삭제)."""
        if (time.monotonic() - self._last_maintenance) < self._maintenance_gap:
            return
        self._last_maintenance = time.monotonic()
        try:
            res = self.db.maintenance(purge_html=True, prune_done_days=40, do_vacuum=False)
            self.log(f"  🧹 DB 정리: {res}")
        except Exception as e:
            self.log(f"  ! DB 정리 실패: {e}")

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

    def _studio_blocked(self) -> bool:
        """카페 스튜디오 본인인증이 없어 회원검색이 막힌 상태면 10분간 시도 안 함."""
        st = cafe_api.STUDIO_STATE
        return bool(st.get("need_auth")) and (time.time() - st.get("at", 0)) < 600

    def _resolve_some(self):
        """매 사이클 pending 회원을 소량씩 해석 (백그라운드 점진 처리)."""
        if not self.session_ok or self._studio_blocked():
            return
        try:
            from .company_matcher import resolve_pending
            done = resolve_pending(self.db, self.client, batch=5, min_gap_s=0.3, log=self.log)
            if done:
                self._emit("resolve_progress", self.db.resolution_stats())
        except Exception as e:
            self.log(f"  ! 회원 해석 실패: {e}")

    # ── 목록 순회 (재방문 스케줄 사이의 공백 보완) ──────────────────────────
    # 고정 스케줄(12h~7d, 30d) 사이에 제목·본문·댓글을 바꾸면 다음 방문까지 못 본다.
    # 전체글 목록 API는 글마다 현재 제목·댓글수·본문앞부분(summary)을 싸게 주므로,
    # 별도 스레드에서 계속 훑어 달라진 글만 즉시 재방문 예약한다(정밀 비교는 재방문).
    #  - 일반 순회: 최근 SWEEP_DAYS일, 한 바퀴 후 SWEEP_REST_S 휴식
    #  - 깊은 순회: 하루 한 번, 목록 API 한도(DEEP_MAX_PAGES)까지 — 오래된 글 수정 대비
    #  - 목록에서 사라진 글(삭제·비공개 전환)도 재방문해 상태 확인
    #  - DB에 없는 글(서버 중단 누락)은 큐에 넣고 메인 루프가 수집
    #  - 진행 상황은 meta(sweep_last_<cafe_id>)에 기록 → 대시보드 상태 표시
    SWEEP_DAYS = 45
    SWEEP_GAP_S = 1.2            # 순회 요청 간격
    SWEEP_REST_S = 2 * 3600      # 한 바퀴 끝나면 쉬는 시간
    DEEP_EVERY_S = 24 * 3600     # 깊은 순회 주기
    DEEP_DAYS = 365
    DEEP_MAX_PAGES = 1000        # 목록 API 5만건 상한(50건×1000p)
    SWEEP_MISSING_PER_CYCLE = 10

    def _start_sweep(self):
        if self._sweep_thread is not None and self._sweep_thread.is_alive():
            return
        if self._sweep_thread is not None:
            self.log("  ⚠ 순회 스레드가 멈춰 있어 다시 시작합니다")
        self._sweep_thread = threading.Thread(target=self._sweep_loop, name="sweep", daemon=True)
        self._sweep_thread.start()

    def _sweep_loop(self):
        import httpx
        time.sleep(20)
        db = Database(self.db.path)
        db.conn.execute("PRAGMA busy_timeout=15000")
        last_deep = float(db.get_meta("sweep_last_deep") or 0)
        while True:
            try:
                deep = (time.time() - last_deep) >= self.DEEP_EVERY_S
                # 세션 쿠키는 바퀴마다 메인 클라이언트에서 복사(재로그인 반영)
                client = httpx.Client(headers=self.client.headers, cookies=self.client.cookies,
                                      timeout=12.0, follow_redirects=True)
                try:
                    for t in self.targets:
                        try:
                            self._sweep_cafe(db, client, t, deep)
                        except Exception as e:
                            self.log(f"  ! 순회 실패 [{t.name}]: {e}")
                finally:
                    client.close()
                if deep:
                    last_deep = time.time()
                    db.set_meta("sweep_last_deep", str(last_deep))
            except Exception as e:      # 스레드가 죽지 않게 — 무엇이든 기록 후 계속
                self.log(f"  ! 순회 루프 오류: {e}")
                time.sleep(60)
                continue
            time.sleep(self.SWEEP_REST_S)

    def _sweep_cafe(self, db: Database, client, t: CafeTarget, deep: bool = False):
        import json
        days = self.DEEP_DAYS if deep else self.SWEEP_DAYS
        max_pages = self.DEEP_MAX_PAGES if deep else 3000
        cutoff = (time.time() - days * 86400) * 1000
        today = time.strftime("%Y%m%d")
        started = time.time()
        page, hits, miss, gone, errs = 1, 0, 0, 0, 0
        while page <= max_pages:
            if page % 20 == 1:   # 진행 중 표시(대시보드가 '기록없음' 경고 대신 '진행중'으로 보이게)
                db.set_meta(f"sweep_run_{t.cafe_id}", json.dumps(
                    {"started": started, "page": page, "deep": deep, "at": time.time()}))
            time.sleep(self.SWEEP_GAP_S)
            try:
                arts = cafe_api.fetch_article_list(t.cafe_id, menu_id=0, per_page=50,
                                                   page=page, client=client)
            except Exception as e:
                errs += 1
                if errs >= 3:
                    self.log(f"  ! 순회 중단 [{t.name}] p{page}: {e}")
                    break
                time.sleep(30)
                continue
            changed, missing = db.sweep_compare(t.cafe_id, arts)
            for aid, why in changed:
                db.enqueue_revisit_now(t.cafe_id, aid)
                self.log(f"  ⟳ SWEEP [{t.name}][{aid}] {why} 변동 → 즉시 재방문")
            for a in missing:
                self._sweep_missing.put((t, a))
            # 목록에서 사라진 글: 같은 날 한 번만 재방문 예약(삭제·비공개 전환 확인)
            recent = (time.time() - self.SWEEP_DAYS * 86400) * 1000
            dated0 = [a.write_ts for a in arts if a.write_ts and not a.is_notice]
            for aid in (db.sweep_gone(t.cafe_id, arts) if dated0 and min(dated0) >= recent else []):
                db.enqueue_revisit_now(t.cafe_id, aid, phase=f"gone{today}")
                gone += 1
            hits += len(changed)
            miss += len(missing)
            dated = [a.write_ts for a in arts if a.write_ts and not a.is_notice]
            if not arts or len(arts) < 50 or (dated and min(dated) < cutoff):
                break
            page += 1
        kind = "깊은순회" if deep else "순회"
        self.log(f"  ⟳ SWEEP [{t.name}] {kind} 완료 {page}p · 변동 {hits} · 누락 {miss} · 사라짐확인 {gone}")
        db.set_meta(f"sweep_last_{t.cafe_id}", json.dumps({
            "name": t.name, "finished": time.time(), "secs": round(time.time() - started),
            "pages": page, "changed": hits, "missing": miss, "gone": gone,
            "deep": deep, "errors": errs}, ensure_ascii=False))

    def _drain_sweep_missing(self):
        """순회에서 발견한 미수집 글을 메인 루프에서 조금씩 수집(본문·댓글·업체매칭).
        겸사겸사 순회 스레드가 살아있는지 확인해 죽었으면 다시 띄운다."""
        self._start_sweep()
        for _ in range(self.SWEEP_MISSING_PER_CYCLE):
            try:
                t, a = self._sweep_missing.get_nowait()
            except queue.Empty:
                return
            try:
                if self.db.upsert_article(a, session_ok=self.session_ok):
                    self._crawl_new(a, t)
            except Exception as e:
                self.log(f"  ! 누락글 수집 실패 [{a.article_id}]: {e}")

    def _maybe_retry_notmember(self):
        """6시간마다 오래된 not_member 를 pending 으로 되돌려 재해석(자가치유).
        세션 만료 등으로 한때 실패했던 업체 아이디가 영구 누락되지 않게 한다."""
        if not self.session_ok:
            return
        if (time.monotonic() - self._last_notmember_retry) < self._notmember_gap:
            return
        if self._studio_blocked():
            return
        self._last_notmember_retry = time.monotonic()
        try:
            n = self.db.requeue_not_member(limit=300)
            if n:
                self.log(f"  ↻ not_member {n}건 재해석 대기열로 복귀")
        except Exception as e:
            self.log(f"  ! not_member 재큐 실패: {e}")


def build_watcher(db: Database, session_dir: Path, account_id: str | None,
                  targets: list[CafeTarget], **kwargs) -> Watcher:
    sm = SessionManager(session_dir)
    cookies = None
    if account_id and sm.verify(account_id).ok:
        cookies = sm.load_cookies(account_id)
    client = cafe_api.make_client(cookies)
    # session_mgr/account를 넘겨야 워처가 갱신된 쿠키를 파일로 되쓴다(재시작해도 세션 유지).
    kwargs.setdefault("session_mgr", sm)
    kwargs.setdefault("account", account_id)
    return Watcher(db, client, targets, **kwargs)
