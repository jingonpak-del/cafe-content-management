"""FastAPI 서버 — 카페 게시글 감시 시스템."""
from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .db import Database, now_ms
from .session import SessionManager

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "gamsi.db"
CONFIG_PATH = ROOT / "config" / "settings.json"
SESSION_DIR = ROOT / "data" / "sessions"
STATIC = Path(__file__).resolve().parent / "static"

app = FastAPI(title="카페 게시글 감시 시스템")

# ── 통합 SSO: 대시보드(dashboard.whitedr.com) 로그인 공유 ──────────────────
import base64 as _b64, hashlib as _hl, hmac as _hm

_SSO_SECRET_PATH = Path(__file__).resolve().parents[1] / "config" / "sso_secret.txt"
_SSO_SECRET = _SSO_SECRET_PATH.read_text(encoding="utf-8").strip().encode() if _SSO_SECRET_PATH.exists() else None


def _sso_verify(cookie):
    if not _SSO_SECRET or not cookie or "." not in cookie:
        return None
    body, sig = cookie.rsplit(".", 1)
    if not _hm.compare_digest(sig, _hm.new(_SSO_SECRET, body.encode(), _hl.sha256).hexdigest()):
        return None
    try:
        p = json.loads(_b64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    except Exception:
        return None
    return p if p.get("exp", 0) >= time.time() else None


@app.middleware("http")
async def _sso_gate(request, call_next):
    host = (request.headers.get("host") or "").split(":")[0]
    public = host.endswith("whitedr.com")     # 공개(터널) 접근만 SSO 강제, localhost 개발은 면제
    if public:
        p = _sso_verify(request.cookies.get("sso"))
        if not (p and (p.get("admin") or "checker" in (p.get("perms") or []))):
            return HTMLResponse(
                '<div style="font-family:sans-serif;padding:40px;text-align:center">'
                '<h3>🔒 접근 권한이 없습니다</h3><p><a href="https://dashboard.whitedr.com/login">'
                '대시보드에서 master로 로그인</a> 후 이용하세요.</p></div>', status_code=403)
    resp = await call_next(request)
    if public:
        resp.headers["Content-Security-Policy"] = "frame-ancestors https://dashboard.whitedr.com"
        if "x-frame-options" in resp.headers:
            del resp.headers["x-frame-options"]
    resp.headers["Cache-Control"] = "no-store"   # 브라우저 캐시로 인한 stale(빈 화면) 방지
    return resp


STATE = {
    "session_ok": False,
    "cafe_names": [],   # [{"cafe_id":..,"name":..}]
    "account_id": "",
    "running": False,
}

# 드라이버(브라우저)를 요청 간에 유지해야 하므로 SessionManager는 싱글톤으로 보관.
SESSION_MGR = SessionManager(SESSION_DIR)


def _db() -> Database:
    return Database(DB_PATH)


def _fmt(ms) -> str:
    if not ms:
        return ""
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M")


def _load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"cafes": [], "account_id": "", "tick_s": 1.5}


def _save_config(cfg: dict):
    CONFIG_PATH.write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _cafe_names() -> dict[int, str]:
    cfg = _load_config()
    return {c["cafe_id"]: c.get("name") or c.get("cafe_url", "") for c in cfg.get("cafes", [])}


def _cafe_urls() -> dict[int, str]:
    cfg = _load_config()
    return {c["cafe_id"]: c.get("cafe_url", "") for c in cfg.get("cafes", [])}


def _member_page_url(cafe_url: str, cafe_id: int, member_key: str) -> str:
    """작성자 카페 회원 페이지 (작성글 목록). 브라우저가 로그인 쿠키로 렌더링."""
    if not (cafe_url and member_key):
        return ""
    return (f"https://cafe.naver.com/{cafe_url}.cafe"
            f"?iframe_url=/ca-fe/cafes/{cafe_id}/members/{member_key}")


# ── WebSocket Hub ─────────────────────────────────────────────────────────────

class Hub:
    def __init__(self):
        self.clients: set[WebSocket] = set()
        self.loop: asyncio.AbstractEventLoop | None = None

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)

    async def _broadcast(self, msg: dict):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    def emit(self, kind: str, payload: dict):
        if self.loop:
            asyncio.run_coroutine_threadsafe(
                self._broadcast({"type": kind, **payload}), self.loop
            )


hub = Hub()


# ── 라우트 ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def index():
    return (STATIC / "index.html").read_text(encoding="utf-8")


@app.get("/api/stats")
def stats():
    db = _db()
    try:
        s = db.get_stats()
        s["session_ok"] = STATE["session_ok"]
        s["cafe_names"] = STATE["cafe_names"]
        s["running"] = STATE["running"]
        return s
    finally:
        db.close()


@app.get("/api/export")
def export_csv(cafe_id: int = 0, start: str = "", end: str = "", status: str = ""):
    """기간·카페별 게시글을 CSV(엑셀)로 스트리밍 다운로드."""
    import csv, io as _io
    start_ms = end_ms = None
    try:
        if start:
            start_ms = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
        if end:
            e = datetime.strptime(end, "%Y-%m-%d")
            end_ms = int((e.timestamp() + 86400) * 1000) - 1
    except ValueError:
        return JSONResponse({"error": "날짜 형식은 YYYY-MM-DD"}, status_code=400)

    names = _cafe_names()
    urls = _cafe_urls()
    HEADERS = ["카페", "게시판", "작성일", "상태", "삭제유형", "삭제자", "최초제목", "현재제목", "제목변경",
               "작성자", "멤버태그", "마스킹ID", "등급", "업체명",
               "조회수", "댓글수", "좋아요", "삭제일", "게시글링크", "작성자페이지",
               "본문", "댓글"]

    def _status_kr(r):
        if r["status"] == "deleted" and r["title_changed"]:
            return "변경후삭제"
        if r["status"] == "deleted":
            return "삭제"
        if r["title_changed"]:
            return "제목변경"
        return "정상"

    def _del_kr(r):
        dt = r.get("delete_type")
        if dt == "staff":
            return "스탭삭제"
        if dt == "self":
            return "자삭"
        return "" if r["status"] != "deleted" else "미정"

    def generate():
        db = _db()
        try:
            buf = _io.StringIO()
            w = csv.writer(buf)
            buf.write("﻿")   # 엑셀 UTF-8 인식용 BOM
            w.writerow(HEADERS)
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)
            for r in db.iter_export_rows(cafe_id or None, start_ms, end_ms, status):
                cid = r["cafe_id"]
                masked = ""  # 개별 마스킹ID는 member_cache/company_member에 있으나 조인 생략
                w.writerow([
                    names.get(cid, str(cid)), r.get("menu_name", ""),
                    _fmt(r.get("write_ts")), _status_kr(r),
                    _del_kr(r), r.get("deleted_by_nick", "") or "",
                    r.get("title", ""), r.get("current_title", ""),
                    "Y" if r.get("title_changed") else "",
                    r.get("writer_nickname", ""), r.get("member_tag", ""), masked,
                    r.get("member_level", ""), r.get("company_name", ""),
                    r.get("cur_read", 0), r.get("cur_comment", 0), r.get("cur_like", 0),
                    _fmt(r.get("deleted_at")),
                    f"https://cafe.naver.com/ca-fe/cafes/{cid}/articles/{r['article_id']}",
                    _member_page_url(urls.get(cid, ""), cid, r.get("member_key", "")),
                    (r.get("content_text") or "").strip(),
                    db.get_comments_text(cid, r["article_id"]) if (r.get("cur_comment") or 0) > 0 else "",
                ])
                yield buf.getvalue(); buf.seek(0); buf.truncate(0)
        finally:
            db.close()

    from datetime import datetime as _dt
    cafe_tag = names.get(cafe_id, "전체") if cafe_id else "전체"
    fname = f"cafe_export_{cafe_tag}_{start or 'all'}_{end or 'all'}.csv"
    from urllib.parse import quote as _q
    return StreamingResponse(
        generate(), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{_q(fname)}"},
    )


@app.get("/api/maintenance/status")
def maintenance_status():
    db = _db()
    try:
        return db.db_size_info()
    finally:
        db.close()


@app.post("/api/maintenance/dedupe")
async def maintenance_dedupe():
    """기존 전체 데이터에 중복글 판정 일괄 적용."""
    db = _db()
    try:
        return {"ok": True, **db.backfill_duplicates()}
    finally:
        db.close()


@app.get("/api/past/status")
def past_status():
    """과거글 수집 진행 상황."""
    db = _db()
    try:
        cfg = _load_config()
        out = []
        for c in cfg.get("cafes", []):
            cid = c["cafe_id"]
            phase = db.get_meta(f"past_{cid}_phase") or "list"
            page = db.get_meta(f"past_{cid}_page") or "2"
            num = db.get_meta(f"past_{cid}_num") or "-"
            today = db.get_meta(f"past_{cid}_today") or "-"
            rng = db.conn.execute(
                "SELECT MIN(article_id), MAX(article_id), COUNT(*) FROM articles WHERE cafe_id=?",
                (cid,)).fetchone()
            out.append({
                "cafe_id": cid, "name": c.get("name", ""),
                "phase": phase, "list_page": page, "number_cursor": num,
                "today": today, "min_id": rng[0], "max_id": rng[1], "count": rng[2],
            })
        return out
    finally:
        db.close()


@app.post("/api/maintenance/run")
async def maintenance_run(req: dict = None):
    """DB 정리 실행 (content_html 제거 + 오래된 완료 스케줄 삭제 + 선택적 VACUUM)."""
    req = req or {}
    db = _db()
    try:
        res = db.maintenance(
            purge_html=req.get("purge_html", True),
            prune_done_days=req.get("prune_done_days", 40),
            do_vacuum=req.get("vacuum", False),
        )
        res["after"] = db.db_size_info()
        return {"ok": True, **res}
    finally:
        db.close()


@app.get("/api/boards")
def boards(cafe_id: int = 0):
    """카페별 게시판 목록 (필터 드롭다운용)."""
    db = _db()
    try:
        names = _cafe_names()
        rows = db.get_boards(cafe_id or None)
        for r in rows:
            r["cafe_name"] = names.get(r["cafe_id"], str(r["cafe_id"]))
        return rows
    finally:
        db.close()


@app.get("/api/articles")
def articles(
    search: str = "",
    status: str = "",
    cafe_id: int = 0,
    menu_id: int = -1,
    limit: int = 100,
    offset: int = 0,
):
    db = _db()
    try:
        names = _cafe_names()
        urls = _cafe_urls()
        rows = db.get_articles(
            limit=limit, offset=offset, search=search, status_filter=status,
            cafe_id=cafe_id or None,
            menu_id=(menu_id if menu_id >= 0 else None),
        )
        for r in rows:
            r["write_str"] = _fmt(r.get("write_ts"))
            r["seen_str"] = _fmt(r.get("first_seen_at"))
            r["cafe_name"] = names.get(r["cafe_id"], str(r["cafe_id"]))
            r["url"] = (f"https://cafe.naver.com/ca-fe/cafes/{r['cafe_id']}"
                        f"/articles/{r['article_id']}")
            r["member_url"] = _member_page_url(urls.get(r["cafe_id"], ""), r["cafe_id"], r.get("member_key", ""))
        return {"rows": rows, "has_more": len(rows) == limit}
    finally:
        db.close()


@app.get("/api/flagged")
def flagged(
    type: str = "all",
    limit: int = 100,
    offset: int = 0,
    search: str = "",
    cafe_id: int = 0,
):
    db = _db()
    try:
        names = _cafe_names()
        urls = _cafe_urls()
        rows = db.get_flagged(flag_type=type, limit=limit, offset=offset,
                              search=search.strip(), cafe_id=cafe_id or None)
        for r in rows:
            r["write_str"] = _fmt(r.get("write_ts"))
            r["flag_str"] = _fmt(r.get("flag_ts") or r.get("write_ts"))
            r["cafe_name"] = names.get(r["cafe_id"], str(r["cafe_id"]))
            r["url"] = (f"https://cafe.naver.com/ca-fe/cafes/{r['cafe_id']}"
                        f"/articles/{r['article_id']}")
            r["member_url"] = _member_page_url(urls.get(r["cafe_id"], ""), r["cafe_id"], r.get("member_key", ""))
        return {"rows": rows, "has_more": len(rows) == limit}
    finally:
        db.close()


@app.post("/api/flagged/review")
async def flagged_review(req: dict):
    """이상감지 항목 확인 토글."""
    cafe_id = req.get("cafe_id")
    article_id = req.get("article_id")
    reviewed = bool(req.get("reviewed", True))
    if cafe_id is None or article_id is None:
        return JSONResponse({"error": "cafe_id, article_id required"}, status_code=400)
    db = _db()
    try:
        db.set_reviewed(int(cafe_id), int(article_id), reviewed)
        return {"ok": True}
    finally:
        db.close()


@app.post("/api/flagged/review_batch")
async def flagged_review_batch(req: dict):
    """여러 이상감지 항목을 한 번에 확인 처리."""
    items = req.get("items") or []
    reviewed = bool(req.get("reviewed", True))
    items = [{"cafe_id": int(i["cafe_id"]), "article_id": int(i["article_id"])}
             for i in items if i.get("cafe_id") is not None and i.get("article_id") is not None]
    if not items:
        return {"ok": True, "count": 0}
    db = _db()
    try:
        n = db.set_reviewed_batch(items, reviewed)
        return {"ok": True, "count": n}
    finally:
        db.close()


@app.get("/api/articles/{cafe_id}/{article_id}")
def article_detail(cafe_id: int, article_id: int):
    db = _db()
    try:
        d = db.get_article_detail(cafe_id, article_id)
        if not d:
            return JSONResponse({"error": "not found"}, status_code=404)
        d["write_str"] = _fmt(d.get("write_ts"))
        d["deleted_str"] = _fmt(d.get("deleted_at"))
        d["url"] = f"https://cafe.naver.com/ca-fe/cafes/{cafe_id}/articles/{article_id}"
        d["member_url"] = _member_page_url(_cafe_urls().get(cafe_id, ""), cafe_id, d.get("member_key", ""))
        for h in d.get("title_history", []):
            h["changed_str"] = _fmt(h.get("changed_at"))
        # 상태 이력 타임라인 구성 (작성 → 제목변경들 → 삭제)
        timeline = [{"type": "created", "ts": d.get("write_ts"),
                     "str": d.get("write_str"), "text": d.get("title")}]
        for h in d.get("title_history", []):
            timeline.append({"type": "title_changed", "ts": h.get("changed_at"),
                             "str": h.get("changed_str"), "phase": h.get("check_phase"),
                             "old": h.get("old_title"), "text": h.get("new_title")})
        if d.get("status") == "deleted":
            dt = d.get("delete_type")
            if dt == "staff":
                by = d.get("deleted_by_nick") or ""
                del_label = f"스탭삭제 (삭제자: {by})"
            elif dt == "self":
                del_label = "자삭 (작성자 본인)"
            else:
                del_label = "삭제 (유형 확인 중)"
            timeline.append({"type": "deleted", "ts": d.get("deleted_at"),
                             "str": d.get("deleted_str"), "delete_type": dt,
                             "text": del_label})
        timeline.sort(key=lambda x: x.get("ts") or 0)
        d["timeline"] = timeline
        return d
    finally:
        db.close()


# ── 키워드 API ────────────────────────────────────────────────────────────────

@app.get("/api/keywords")
def get_keywords():
    db = _db()
    try:
        rows = [dict(r) for r in db.get_keyword_stats()]
        for r in rows:
            r["last_hit_str"] = _fmt(r.get("last_hit_at"))
        return rows
    finally:
        db.close()


@app.post("/api/keywords")
async def add_keyword(req: dict):
    keyword = (req.get("keyword") or "").strip()
    if not keyword:
        return JSONResponse({"error": "keyword required"}, status_code=400)
    db = _db()
    try:
        kid = db.add_keyword(keyword)
        return {"id": kid, "keyword": keyword}
    finally:
        db.close()


@app.delete("/api/keywords/{keyword_id}")
def delete_keyword(keyword_id: int):
    db = _db()
    try:
        db.delete_keyword(keyword_id)
        return {"ok": True}
    finally:
        db.close()


@app.patch("/api/keywords/{keyword_id}")
async def toggle_keyword(keyword_id: int, req: dict):
    db = _db()
    try:
        db.toggle_keyword(keyword_id, bool(req.get("is_active", True)))
        return {"ok": True}
    finally:
        db.close()


# ── 업체 API ──────────────────────────────────────────────────────────────────

@app.get("/api/companies")
def companies(start: str = "", end: str = ""):
    """업체 통계. start/end='YYYY-MM-DD' 작성일 범위 필터 (선택)."""
    start_ms = end_ms = None
    try:
        if start:
            start_ms = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
        if end:
            # end 날짜의 23:59:59.999 까지 포함
            e = datetime.strptime(end, "%Y-%m-%d")
            end_ms = int((e.timestamp() + 86400) * 1000) - 1
    except ValueError:
        return JSONResponse({"error": "날짜 형식은 YYYY-MM-DD"}, status_code=400)
    db = _db()
    try:
        rows = [dict(r) for r in db.get_company_stats(start_ms, end_ms)]
        for r in rows:
            r["last_write_str"] = _fmt(r.get("last_write_ts"))
        return rows
    finally:
        db.close()


@app.get("/api/companies/articles")
def company_articles(name: str, start: str = "", end: str = ""):
    """특정 업체의 게시글 리스트 (작성일 범위 선택)."""
    if not name:
        return JSONResponse({"error": "name required"}, status_code=400)
    start_ms = end_ms = None
    try:
        if start:
            start_ms = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
        if end:
            e = datetime.strptime(end, "%Y-%m-%d")
            end_ms = int((e.timestamp() + 86400) * 1000) - 1
    except ValueError:
        return JSONResponse({"error": "날짜 형식은 YYYY-MM-DD"}, status_code=400)
    db = _db()
    try:
        names = _cafe_names()
        rows = db.get_company_articles(name, start_ms, end_ms)
        for r in rows:
            r["write_str"] = _fmt(r.get("write_ts"))
            r["cafe_name"] = names.get(r["cafe_id"], str(r["cafe_id"]))
            r["url"] = (f"https://cafe.naver.com/ca-fe/cafes/{r['cafe_id']}"
                        f"/articles/{r['article_id']}")
            r["member_url"] = _member_page_url(_cafe_urls().get(r["cafe_id"], ""), r["cafe_id"], r.get("member_key", ""))
        return {"rows": rows}
    finally:
        db.close()


@app.get("/api/companies/mapping")
def company_mapping():
    db = _db()
    try:
        return [dict(r) for r in db.get_all_company_mappings()]
    finally:
        db.close()


@app.get("/api/companies/members")
def company_members():
    """업체 아이디 리스트 (구글시트 연동 아이디 → 멤버태그·최근닉네임·마지막작성일)."""
    db = _db()
    try:
        names = _cafe_names()
        rows = db.get_company_member_list()
        for r in rows:
            r["cafe_name"] = names.get(r["cafe_id"], str(r["cafe_id"]))
            r["nickname"] = r.get("last_nick") or r.get("resolved_nick") or ""
            r["last_write_str"] = _fmt(r.get("last_write_ts"))
        return rows
    finally:
        db.close()


# ── 서포터즈 분석 ─────────────────────────────────────────────────────────────

def _date_range(start: str, end: str):
    s = e = None
    if start:
        s = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
    if end:
        e = int((datetime.strptime(end, "%Y-%m-%d").timestamp() + 86400) * 1000) - 1
    return s, e


SUP_REFRESH = {"running": False, "done": 0, "total": 0, "group_id": None}


@app.get("/api/supporters/groups")
def sup_groups():
    db = _db()
    try:
        return db.get_supporter_groups()
    finally:
        db.close()


@app.post("/api/supporters/groups")
async def sup_group_add(req: dict):
    name = (req.get("name") or "").strip()
    if not name:
        return JSONResponse({"error": "name required"}, status_code=400)
    db = _db()
    try:
        return {"ok": True, "id": db.create_supporter_group(name)}
    finally:
        db.close()


@app.delete("/api/supporters/groups/{group_id}")
def sup_group_del(group_id: int):
    db = _db()
    try:
        db.delete_supporter_group(group_id)
        return {"ok": True}
    finally:
        db.close()


@app.get("/api/supporters/{group_id}/members")
def sup_members(group_id: int):
    db = _db()
    try:
        names = _cafe_names()
        rows = db.get_supporter_members(group_id)
        for r in rows:
            r["cafe_name"] = names.get(r["cafe_id"], str(r["cafe_id"]))
        return rows
    finally:
        db.close()


@app.post("/api/supporters/{group_id}/members/bulk")
async def sup_members_bulk(group_id: int, req: dict):
    """아이디/닉네임/멤버태그 여러 개를 검색해 그룹원으로 일괄 등록."""
    ids = [s.strip() for s in (req.get("ids") or []) if s and s.strip()]
    id_type = int(req.get("id_type", 0))
    only_cafe = int(req.get("cafe_id", 0))
    if not ids:
        return {"ok": True, "registered": 0, "not_found": []}
    from . import cafe_api
    cfg = _load_config()
    account_id = cfg.get("account_id")
    cookies = SESSION_MGR.load_cookies(account_id) if account_id and SESSION_MGR.verify(account_id).ok else None
    if not cookies:
        return JSONResponse({"error": "로그인 세션이 필요합니다"}, status_code=400)
    client = cafe_api.make_client(cookies)
    db = _db()
    registered = 0
    not_found = []
    try:
        for kw in ids:
            hit = False
            for c in cfg.get("cafes", []):
                cid = c["cafe_id"]
                if only_cafe and cid != only_cafe:
                    continue
                try:
                    members = cafe_api.search_members(cid, kw, id_type=id_type, per_page=10, client=client)
                except Exception:
                    members = []
                for m in members:
                    db.add_supporter_member(group_id, cid, m.member_key, kw if id_type == 0 else "",
                                            m.nickname, m.member_tag, m.masked_id)
                    registered += 1
                    hit = True
            if not hit:
                not_found.append(kw)
        return {"ok": True, "registered": registered, "not_found": not_found}
    finally:
        client.close()
        db.close()


@app.delete("/api/supporters/{group_id}/members/{cafe_id}/{member_key}")
def sup_member_del(group_id: int, cafe_id: int, member_key: str):
    db = _db()
    try:
        db.remove_supporter_member(group_id, cafe_id, member_key)
        return {"ok": True}
    finally:
        db.close()


@app.get("/api/supporters/{group_id}/boards")
def sup_boards(group_id: int):
    db = _db()
    try:
        return db.get_supporter_boards(group_id)
    finally:
        db.close()


@app.get("/api/supporters/{group_id}/stats")
def sup_stats(group_id: int, start: str = "", end: str = "", menu_id: int = -1):
    try:
        s, e = _date_range(start, end)
    except ValueError:
        return JSONResponse({"error": "날짜 형식 YYYY-MM-DD"}, status_code=400)
    db = _db()
    try:
        names = _cafe_names()
        rows = db.supporter_stats(group_id, s, e, menu_id if menu_id >= 0 else None)
        for r in rows:
            r["cafe_name"] = names.get(r["cafe_id"], str(r["cafe_id"]))
        return rows
    finally:
        db.close()


@app.get("/api/supporters/{group_id}/articles")
def sup_articles(group_id: int, start: str = "", end: str = "", menu_id: int = -1):
    try:
        s, e = _date_range(start, end)
    except ValueError:
        return JSONResponse({"error": "날짜 형식 YYYY-MM-DD"}, status_code=400)
    db = _db()
    try:
        names = _cafe_names()
        urls = _cafe_urls()
        rows = db.supporter_articles(group_id, s, e, menu_id if menu_id >= 0 else None)
        for r in rows:
            cid = r["cafe_id"]
            r["cafe_name"] = names.get(cid, str(cid))
            r["write_str"] = _fmt(r.get("write_ts"))
            r["url"] = f"https://cafe.naver.com/ca-fe/cafes/{cid}/articles/{r['article_id']}"
            r["short_url"] = (f"https://cafe.naver.com/{urls.get(cid,'')}/{r['article_id']}"
                              if urls.get(cid) else r["url"])
        return {"rows": rows}
    finally:
        db.close()


@app.get("/api/supporters/refresh_status")
def sup_refresh_status():
    return SUP_REFRESH


@app.post("/api/supporters/{group_id}/refresh")
async def sup_refresh(group_id: int, req: dict = None):
    """그룹원 게시글을 재크롤해 조회·좋아요·댓글·사진수·글자수를 최신화 (백그라운드)."""
    if SUP_REFRESH["running"]:
        return {"ok": False, "message": "이미 갱신 중"}
    req = req or {}
    try:
        s, e = _date_range(req.get("start", ""), req.get("end", ""))
    except ValueError:
        return JSONResponse({"error": "날짜 형식 YYYY-MM-DD"}, status_code=400)
    menu_id = int(req.get("menu_id", -1))
    threading.Thread(target=_run_sup_refresh, args=(group_id, s, e, menu_id), daemon=True).start()
    return {"ok": True}


def _run_sup_refresh(group_id, s, e, menu_id):
    from . import cafe_api
    cfg = _load_config()
    account_id = cfg.get("account_id")
    cookies = SESSION_MGR.load_cookies(account_id) if account_id and SESSION_MGR.verify(account_id).ok else None
    db = Database(DB_PATH)
    SUP_REFRESH.update(running=True, done=0, total=0, group_id=group_id)
    try:
        arts = db.supporter_articles(group_id, s, e, menu_id if menu_id >= 0 else None)
        SUP_REFRESH["total"] = len(arts)
        if not cookies:
            return
        client = cafe_api.make_client(cookies)
        try:
            for a in arts:
                cid, aid = a["cafe_id"], a["article_id"]
                try:
                    body = cafe_api.fetch_article_body(cid, aid, menu_id=a.get("menu_id") or 0, client=client)
                    db.save_body(body)
                    comments = cafe_api.fetch_comments(cid, aid, client=client)
                    db.save_comments(cid, aid, comments, phase="refresh")
                    db.update_revisit_counts(cid, aid, body.read_count, body.comment_count)
                except cafe_api.ArticleGoneError:
                    db.mark_deleted(cid, aid)
                except Exception:
                    pass
                SUP_REFRESH["done"] += 1
                time.sleep(0.3)
        finally:
            client.close()
    finally:
        SUP_REFRESH["running"] = False
        db.close()


@app.post("/api/companies/mapping")
async def add_company_mapping(req: dict):
    naver_id = (req.get("naver_id") or "").strip()
    company_name = (req.get("company_name") or "").strip()
    if not naver_id or not company_name:
        return JSONResponse({"error": "naver_id and company_name required"}, status_code=400)
    db = _db()
    try:
        db.upsert_company_mapping(naver_id, company_name)
        return {"ok": True}
    finally:
        db.close()


# ── 글관리: 등록 작성자 ───────────────────────────────────────────────────────

@app.post("/api/writers/search")
async def writers_search(req: dict):
    """닉네임/아이디/멤버코드로 작성자 검색 (등록된 카페 전체)."""
    keyword = (req.get("keyword") or "").strip()
    id_type = int(req.get("id_type", 0))   # 0=아이디 1=닉네임 3=멤버태그
    only_cafe = int(req.get("cafe_id", 0))
    if not keyword:
        return JSONResponse({"error": "keyword required"}, status_code=400)
    from . import cafe_api
    cfg = _load_config()
    account_id = cfg.get("account_id")
    cookies = SESSION_MGR.load_cookies(account_id) if account_id and SESSION_MGR.verify(account_id).ok else None
    if not cookies:
        return JSONResponse({"error": "로그인 세션이 필요합니다 (설정에서 로그인)"}, status_code=400)
    client = cafe_api.make_client(cookies)
    names = _cafe_names()
    results = []
    try:
        for c in cfg.get("cafes", []):
            cid = c["cafe_id"]
            if only_cafe and cid != only_cafe:
                continue
            try:
                members = cafe_api.search_members(cid, keyword, id_type=id_type, per_page=10, client=client)
                for m in members:
                    results.append({
                        "cafe_id": cid, "cafe_name": names.get(cid, str(cid)),
                        "member_key": m.member_key, "member_tag": m.member_tag,
                        "nickname": m.nickname, "masked_id": m.masked_id,
                        "level_name": m.level_name,
                    })
            except Exception:
                continue
    finally:
        client.close()
    return {"results": results}


@app.post("/api/writers")
async def writers_add(req: dict):
    db = _db()
    try:
        gid = req.get("group_id")
        db.add_watched_writer(
            int(req["cafe_id"]), req["member_key"], req.get("naver_id", ""),
            req.get("nickname", ""), req.get("masked_id", ""), req.get("member_tag", ""),
            req.get("level_name", ""), req.get("memo", ""),
            group_id=int(gid) if gid not in (None, "", 0, "0") else None,
        )
        return {"ok": True}
    finally:
        db.close()


@app.post("/api/writers/bulk")
async def writers_add_bulk(req: dict):
    """여러 아이디/닉네임/멤버태그를 한 번에 검색해서 일괄 등록.
    ids: 문자열 리스트. id_type: 0=아이디 1=닉네임 3=멤버태그."""
    ids = req.get("ids") or []
    ids = [s.strip() for s in ids if s and s.strip()]
    id_type = int(req.get("id_type", 0))
    only_cafe = int(req.get("cafe_id", 0))
    gid = req.get("group_id")
    group_id = int(gid) if gid not in (None, "", 0, "0") else None
    if not ids:
        return {"ok": True, "registered": 0, "not_found": []}
    from . import cafe_api
    cfg = _load_config()
    account_id = cfg.get("account_id")
    cookies = SESSION_MGR.load_cookies(account_id) if account_id and SESSION_MGR.verify(account_id).ok else None
    if not cookies:
        return JSONResponse({"error": "로그인 세션이 필요합니다"}, status_code=400)
    client = cafe_api.make_client(cookies)
    db = _db()
    registered = 0
    not_found = []
    try:
        for kw in ids:
            hit = False
            for c in cfg.get("cafes", []):
                cid = c["cafe_id"]
                if only_cafe and cid != only_cafe:
                    continue
                try:
                    members = cafe_api.search_members(cid, kw, id_type=id_type, per_page=10, client=client)
                except Exception:
                    members = []
                for m in members:
                    db.add_watched_writer(
                        cid, m.member_key, "", m.nickname, m.masked_id,
                        m.member_tag, m.level_name, "", group_id=group_id,
                    )
                    registered += 1
                    hit = True
            if not hit:
                not_found.append(kw)
        return {"ok": True, "registered": registered, "not_found": not_found, "total_ids": len(ids)}
    finally:
        client.close()
        db.close()


# ── 글관리: 작성자 그룹 ────────────────────────────────────────────────────────

@app.get("/api/writer-groups")
def writer_groups_list():
    db = _db()
    try:
        return {"groups": db.list_writer_groups()}
    finally:
        db.close()


@app.post("/api/writer-groups")
async def writer_groups_add(req: dict):
    name = (req.get("name") or "").strip()
    if not name:
        return JSONResponse({"error": "그룹 이름을 입력하세요"}, status_code=400)
    db = _db()
    try:
        return {"ok": True, "id": db.add_writer_group(name)}
    finally:
        db.close()


@app.patch("/api/writer-groups/{group_id}")
async def writer_groups_rename(group_id: int, req: dict):
    name = (req.get("name") or "").strip()
    if not name:
        return JSONResponse({"error": "그룹 이름을 입력하세요"}, status_code=400)
    db = _db()
    try:
        db.rename_writer_group(group_id, name)
        return {"ok": True}
    finally:
        db.close()


@app.delete("/api/writer-groups/{group_id}")
def writer_groups_delete(group_id: int):
    db = _db()
    try:
        db.delete_writer_group(group_id)
        return {"ok": True}
    finally:
        db.close()


@app.post("/api/writers/{cafe_id}/{member_key}/group")
async def writer_set_group(cafe_id: int, member_key: str, req: dict):
    gid = req.get("group_id")
    db = _db()
    try:
        db.set_writer_group(cafe_id, member_key,
                            int(gid) if gid not in (None, "", 0, "0") else None)
        return {"ok": True}
    finally:
        db.close()


@app.get("/api/writers")
def writers_list():
    db = _db()
    try:
        names = _cafe_names()
        rows = db.get_watched_writers()
        for r in rows:
            r["cafe_name"] = names.get(r["cafe_id"], str(r["cafe_id"]))
        return rows
    finally:
        db.close()


@app.delete("/api/writers/{cafe_id}/{member_key}")
def writers_delete(cafe_id: int, member_key: str):
    db = _db()
    try:
        db.remove_watched_writer(cafe_id, member_key)
        return {"ok": True}
    finally:
        db.close()


@app.get("/api/writers/{cafe_id}/{member_key}/articles")
def writer_articles(cafe_id: int, member_key: str, limit: int = 200, offset: int = 0):
    db = _db()
    try:
        names = _cafe_names()
        urls = _cafe_urls()
        cafe_url = urls.get(cafe_id, "")
        rows = db.get_writer_articles(cafe_id, member_key, limit=limit, offset=offset)
        for r in rows:
            r["write_str"] = _fmt(r.get("write_ts"))
            r["cafe_name"] = names.get(r["cafe_id"], str(r["cafe_id"]))
            r["url"] = (f"https://cafe.naver.com/ca-fe/cafes/{r['cafe_id']}"
                        f"/articles/{r['article_id']}")
            # 짧은 링크 (드래그·복사용): https://cafe.naver.com/{cafe_url}/{article_id}
            r["short_url"] = (f"https://cafe.naver.com/{cafe_url}/{r['article_id']}"
                              if cafe_url else r["url"])
        return {"rows": rows, "has_more": len(rows) == limit}
    finally:
        db.close()


# ── 글관리: 과거 전체글 백필 ──────────────────────────────────────────────────
# 실시간 수집 이후 글만 DB에 있으므로, 작성자의 과거 전체 이력을 네이버에서 가져와
# articles 에 채운다(=writer_articles 가 과거+현재 모두 표시). 워처 폴링을 방해하지
# 않도록 별도 스레드/클라이언트로 on-demand 실행.
BACKFILL_JOBS: dict = {}
BACKFILL_LOCK = threading.Lock()
BACKFILL_MAX = 30000       # 다작 계정 상한 (gimhaezumma 최다 1.6만+ 커버)


def _crawl_member_backfill(cafe_id: int, member_key: str, max_n: int):
    from . import cafe_api
    key = (cafe_id, member_key)
    cfg = _load_config()
    account = cfg.get("account_id")
    cookies = (SESSION_MGR.load_cookies(account)
               if account and SESSION_MGR.verify(account).ok else None)
    client = cafe_api.make_client(cookies)
    db = _db()
    db.conn.execute("PRAGMA busy_timeout=15000")
    seen, n, total, page = set(), 0, 0, 1
    try:
        if not cookies:
            raise RuntimeError("로그인 세션이 없습니다 (설정에서 로그인).")
        while page <= 700:
            arts, total = cafe_api.fetch_member_articles(
                cafe_id, member_key, per_page=50, page=page, client=client)
            if not arts:
                break
            fresh = [a for a in arts if a.article_id not in seen]
            if not fresh:
                break
            db.upsert_member_articles(fresh)
            for a in fresh:
                seen.add(a.article_id)
            n += len(fresh)
            with BACKFILL_LOCK:
                job = BACKFILL_JOBS.get(key, {})
                job.update(done=n, total=total)
                cancel = job.get("cancel")
            if cancel or n >= max_n or len(arts) < 50:
                break
            page += 1
            time.sleep(0.15)
        with BACKFILL_LOCK:
            BACKFILL_JOBS[key].update(status="done", done=n, total=total)
    except Exception as e:
        with BACKFILL_LOCK:
            BACKFILL_JOBS[key].update(status="error", error=str(e))
    finally:
        client.close()
        db.close()


@app.post("/api/writers/{cafe_id}/{member_key}/backfill")
def writer_backfill(cafe_id: int, member_key: str):
    """작성자의 과거 전체글을 네이버에서 가져와 DB에 백필(백그라운드 시작)."""
    key = (cafe_id, member_key)
    with BACKFILL_LOCK:
        job = BACKFILL_JOBS.get(key)
        if job and job.get("status") == "running":
            return {"ok": True, "status": "running",
                    "done": job.get("done", 0), "total": job.get("total")}
        BACKFILL_JOBS[key] = {"status": "running", "done": (job or {}).get("done", 0),
                              "total": None, "cancel": False}
    threading.Thread(target=_crawl_member_backfill,
                     args=(cafe_id, member_key, BACKFILL_MAX), daemon=True).start()
    return {"ok": True, "status": "running"}


@app.get("/api/writers/{cafe_id}/{member_key}/backfill/status")
def writer_backfill_status(cafe_id: int, member_key: str):
    with BACKFILL_LOCK:
        job = dict(BACKFILL_JOBS.get((cafe_id, member_key), {})) or None
    return {"job": job}


# ── 구글시트 동기화 / 회원 해석 ───────────────────────────────────────────────

@app.get("/api/sheet/status")
def sheet_status():
    db = _db()
    try:
        cfg = _load_config()
        s = db.resolution_stats()
        s["configured"] = bool(cfg.get("sheet", {}).get("spreadsheet_id"))
        return s
    finally:
        db.close()


@app.post("/api/sheet/config")
async def sheet_config(req: dict):
    cfg = _load_config()
    cfg["sheet"] = {
        "spreadsheet_id": (req.get("spreadsheet_id") or "").strip(),
        "gid": (str(req.get("gid") or "0")).strip(),
    }
    _save_config(cfg)
    return {"ok": True}


@app.post("/api/sheet/backfill")
async def sheet_backfill():
    """resolved된 업체회원으로 기존 게시글 일괄 소급 매칭."""
    db = _db()
    try:
        n = db.backfill_all_resolved()
        return {"ok": True, "matched": n}
    finally:
        db.close()


@app.post("/api/sheet/sync")
async def sheet_sync_now():
    """즉시 시트 동기화 (해석은 백그라운드 워처가 진행)."""
    cfg = _load_config()
    sheet = cfg.get("sheet", {})
    if not sheet.get("spreadsheet_id"):
        return JSONResponse({"error": "시트가 설정되지 않았습니다"}, status_code=400)
    db = _db()
    try:
        from .company_matcher import sync_sheet
        cafe_ids = [c["cafe_id"] for c in cfg.get("cafes", [])]
        res = sync_sheet(db, sheet["spreadsheet_id"], sheet.get("gid", 0), cafe_ids)
        return {"ok": True, **res, **db.resolution_stats()}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    finally:
        db.close()


# ── 설정 API ──────────────────────────────────────────────────────────────────

@app.get("/api/settings")
def get_settings():
    cfg = _load_config()
    return {
        "cafes": cfg.get("cafes", []),
        "account_id": cfg.get("account_id", ""),
        "tick_s": cfg.get("tick_s", 1.5),
    }


@app.post("/api/settings")
async def save_settings(req: dict):
    cfg = _load_config()
    for key in ("cafes", "account_id", "tick_s"):
        if key in req:
            cfg[key] = req[key]
    _save_config(cfg)
    STATE["cafe_names"] = [
        {"cafe_id": c["cafe_id"], "name": c.get("name") or c.get("cafe_url", ""), "cafe_url": c.get("cafe_url", "")}
        for c in cfg.get("cafes", [])
    ]
    STATE["account_id"] = cfg.get("account_id", "")
    return {"ok": True}


@app.post("/api/settings/add_cafe")
async def add_cafe(req: dict):
    cfg = _load_config()
    cafes = cfg.get("cafes", [])
    cafe_url = (req.get("cafe_url") or "").strip()
    cafe_id = int(req.get("cafe_id") or 0)
    if not cafe_url or not cafe_id:
        return JSONResponse({"error": "cafe_url and cafe_id required"}, status_code=400)
    # 중복 방지
    if any(c["cafe_id"] == cafe_id for c in cafes):
        return JSONResponse({"error": "이미 등록된 카페입니다"}, status_code=400)
    cafes.append({
        "cafe_url": cafe_url,
        "cafe_id": cafe_id,
        "menu_id": int(req.get("menu_id") or 0),
        "name": (req.get("name") or cafe_url).strip(),
    })
    cfg["cafes"] = cafes
    _save_config(cfg)
    return {"ok": True, "cafes": cafes}


@app.delete("/api/settings/cafe/{cafe_id}")
def delete_cafe(cafe_id: int):
    cfg = _load_config()
    cfg["cafes"] = [c for c in cfg.get("cafes", []) if c["cafe_id"] != cafe_id]
    _save_config(cfg)
    return {"ok": True}


@app.post("/api/settings/resolve_cafe")
async def resolve_cafe(req: dict):
    cafe_url = (req.get("cafe_url") or "").strip()
    if not cafe_url:
        return JSONResponse({"error": "cafe_url required"}, status_code=400)
    try:
        from . import cafe_api
        club_id = cafe_api.resolve_club_id(cafe_url)
        return {"club_id": club_id}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


# ── 로그인 세션 API ───────────────────────────────────────────────────────────

@app.get("/api/session/status")
def session_status(live: bool = False):
    """세션 상태. live=true면 네이버에 실제 요청을 보내 만료 여부까지 확인."""
    accounts = SESSION_MGR.list_accounts()
    cfg = _load_config()
    account_id = cfg.get("account_id", "")
    if not account_id:
        return {"account_id": "", "ok": False, "reason": "계정 미설정",
                "accounts": accounts, "pending": SESSION_MGR.has_pending_capture()}
    vr = SESSION_MGR.verify_live(account_id) if live else SESSION_MGR.verify(account_id)
    STATE["session_ok"] = vr.ok
    return {"account_id": account_id, "ok": vr.ok, "reason": vr.reason,
            "accounts": accounts, "pending": SESSION_MGR.has_pending_capture()}


_HEALTH_CACHE = {"ts": 0.0, "ok": None, "since": None}

@app.get("/api/session/health")
def session_health():
    """포털 등 외부 대시보드가 폴링하는 경량 상태.
    라이브 확인은 최대 60초에 한 번만 수행(캐시)하여 네이버 부하를 막는다.
    반환: {ok, account_id, expired_since(ms|null), checked_ago_s}."""
    import time as _t
    now = _t.monotonic()
    cfg = _load_config()
    account_id = cfg.get("account_id", "")
    if _HEALTH_CACHE["ok"] is None or (now - _HEALTH_CACHE["ts"]) > 60:
        ok = bool(account_id) and SESSION_MGR.verify_live(account_id).ok
        _HEALTH_CACHE["ts"] = now
        if ok:
            _HEALTH_CACHE["since"] = None
        elif _HEALTH_CACHE["ok"] is not False:
            _HEALTH_CACHE["since"] = now_ms()
        _HEALTH_CACHE["ok"] = ok
        STATE["session_ok"] = ok
    return {
        "ok": bool(_HEALTH_CACHE["ok"]),
        "account_id": account_id,
        "expired_since": _HEALTH_CACHE["since"],
        "checked_ago_s": round(now - _HEALTH_CACHE["ts"], 1),
        "service": "checker",
    }


@app.post("/api/session/capture/start")
async def capture_start(req: dict):
    """1단계: 브라우저를 띄우고 네이버 로그인 페이지로 이동."""
    account_id = (req.get("account_id") or "").strip()
    if not account_id:
        return JSONResponse({"error": "account_id required"}, status_code=400)
    # 계정 ID를 설정에도 저장
    cfg = _load_config()
    cfg["account_id"] = account_id
    _save_config(cfg)
    STATE["account_id"] = account_id
    try:
        SESSION_MGR.start_capture(account_id)
    except Exception as e:
        return JSONResponse(
            {"error": f"브라우저 실행 실패: {e}. Chrome 및 드라이버가 설치돼 있는지 확인하세요."},
            status_code=500,
        )
    return {"ok": True, "message": "브라우저에서 네이버에 로그인한 뒤 '로그인 완료' 버튼을 누르세요."}


@app.post("/api/session/capture/finish")
async def capture_finish():
    """2단계: 열려있는 브라우저에서 쿠키를 캡처해 저장."""
    vr = SESSION_MGR.finish_capture()
    STATE["session_ok"] = vr.ok
    hub.emit("session", {"ok": vr.ok})
    return {"ok": vr.ok, "message": vr.reason}


@app.post("/api/session/capture/cancel")
async def capture_cancel():
    SESSION_MGR._cleanup_driver()
    return {"ok": True}


# ── Watcher 제어 API ──────────────────────────────────────────────────────────

@app.post("/api/watcher/start")
async def start_watcher():
    if STATE["running"]:
        return {"ok": False, "message": "이미 실행 중"}
    cfg = _load_config()
    if not cfg.get("cafes"):
        return JSONResponse({"error": "카페가 설정되지 않았습니다"}, status_code=400)
    threading.Thread(target=_run_watcher, daemon=True).start()
    return {"ok": True}


@app.post("/api/watcher/stop")
async def stop_watcher():
    STATE["running"] = False
    hub.emit("watcher_status", {"running": False})
    return {"ok": True}


def _run_watcher():
    from .watcher import build_watcher, CafeTarget
    cfg = _load_config()
    db = Database(DB_PATH)
    STATE["running"] = True
    hub.emit("watcher_status", {"running": True})

    def on_event(kind: str, payload: dict):
        if kind == "session":
            STATE["session_ok"] = payload.get("ok", False)
        hub.emit(kind, payload)

    targets = [
        CafeTarget(
            cafe_url=c["cafe_url"],
            cafe_id=c["cafe_id"],
            menu_id=c.get("menu_id", 0),
            name=c.get("name") or c["cafe_url"],
        )
        for c in cfg.get("cafes", [])
    ]

    try:
        tick_s = cfg.get("tick_s", 1.5)
        w = build_watcher(
            db=db,
            session_dir=SESSION_DIR,
            account_id=cfg.get("account_id") or None,
            targets=targets,
            on_event=on_event,
            sheet_cfg=cfg.get("sheet", {}),
        )
        STATE["session_ok"] = w.session_ok
        w.run(tick_s=tick_s)
    except Exception as e:
        import traceback
        print(f"Watcher 오류: {e}")
        traceback.print_exc()
    finally:
        STATE["running"] = False
        db.close()
        hub.emit("watcher_status", {"running": False})


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await hub.connect(ws)
    try:
        # 연결 즉시 현재 상태 전송
        await ws.send_json({"type": "state", **STATE})
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        hub.disconnect(ws)


@app.on_event("startup")
async def startup():
    hub.loop = asyncio.get_running_loop()
    cfg = _load_config()
    STATE["cafe_names"] = [
        {"cafe_id": c["cafe_id"], "name": c.get("name") or c.get("cafe_url", ""), "cafe_url": c.get("cafe_url", "")}
        for c in cfg.get("cafes", [])
    ]
    STATE["account_id"] = cfg.get("account_id", "")
    # 카페가 설정돼 있으면 서버 시작 시 감시 자동 재개.
    # 단 GAMSI_NO_WATCH=1 이면 워처를 띄우지 않는다(프로덕션과 gamsi.db·세션을 공유하는
    # 프리뷰/두번째 인스턴스가 중복 수집하지 않도록).
    import os as _os
    if cfg.get("cafes") and not STATE["running"] and _os.environ.get("GAMSI_NO_WATCH") != "1":
        threading.Thread(target=_run_watcher, daemon=True).start()


def main():
    import argparse
    import sys
    for s in (sys.stdout, sys.stderr):
        try:
            s.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
        except Exception:
            pass

    import os
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)))
    p.add_argument("--logfile", default=None, help="로그 파일 경로 (백그라운드 실행용)")
    args = p.parse_args()

    # pythonw(콘솔 없음)로 실행 시 stdout/stderr가 None → uvicorn 로깅이 깨진다.
    # 로그파일이 지정됐거나 stdout이 없으면 파일/널로 우회한다.
    log_target = args.logfile
    if log_target is None and sys.stdout is None:
        log_target = str(ROOT / "data" / "server.log")
    if log_target:
        f = open(log_target, "a", encoding="utf-8", buffering=1)
        sys.stdout = f
        sys.stderr = f

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_config=None)


if __name__ == "__main__":
    main()
