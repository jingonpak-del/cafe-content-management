"""키워드 탐지 — 제목/본문/댓글에서 등록된 키워드를 스캔."""
from __future__ import annotations

from dataclasses import dataclass

from .db import Database


@dataclass
class Hit:
    keyword_id: int
    keyword_text: str
    location: str   # title | content | comment
    snippet: str
    phase: str


def _snippet(text: str, idx: int, kw_len: int, ctx: int = 20) -> str:
    start = max(0, idx - ctx)
    end = min(len(text), idx + kw_len + ctx)
    return text[start:end].replace("\n", " ").strip()


def scan_text(text: str, keywords: list, location: str, phase: str) -> list[Hit]:
    hits: list[Hit] = []
    for kw in keywords:
        kw_text = kw["keyword"] if isinstance(kw, dict) else kw.keyword
        kw_id = kw["id"] if isinstance(kw, dict) else kw.id
        idx = text.lower().find(kw_text.lower())
        if idx >= 0:
            hits.append(Hit(kw_id, kw_text, location, _snippet(text, idx, len(kw_text)), phase))
    return hits


def scan_article(
    db: Database,
    cafe_id: int,
    article_id: int,
    title: str,
    content_text: str,
    comments: list,
    phase: str,
):
    """게시글 제목/본문/댓글 전체 스캔 후 DB에 히트 저장."""
    keywords = [dict(r) for r in db.get_active_keywords()]
    if not keywords:
        return

    all_hits: list[Hit] = []
    all_hits += scan_text(title or "", keywords, "title", phase)
    all_hits += scan_text(content_text or "", keywords, "content", phase)
    for c in comments:
        content = c.content if hasattr(c, "content") else c.get("content", "")
        all_hits += scan_text(content or "", keywords, "comment", phase)

    for hit in all_hits:
        if not db.keyword_hit_exists(hit.keyword_id, cafe_id, article_id, hit.location, phase):
            db.save_keyword_hit(
                hit.keyword_id, hit.keyword_text,
                cafe_id, article_id,
                hit.location, hit.snippet, phase,
            )
