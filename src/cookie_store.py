"""공용 코어(navercafe_core.cookie_store)로 이관됨. 이 파일은 얇은 shim이다.

이관하면서 얻은 것:
  · 원자적 쓰기(tmp→rename) — 저장 중 프로세스가 죽어도 세션 파일이 반토막 나지 않는다
  · list_records() — 저장된 세션의 원래 계정명을 읽는다(파일명은 해시라 역산 불가)

호출부는 `CookieStore(session_dir)` 형태 그대로 쓰면 된다.
"""
from __future__ import annotations

from pathlib import Path

from navercafe_core.cookie_store import (
    SESSION_VERSION,
    CookieStore as _CookieStore,
    SessionRecord,
    _normalize_cookies,
)

from .dpapi import ENTROPY

__all__ = ["CookieStore", "SessionRecord", "SESSION_VERSION", "_normalize_cookies"]


class CookieStore(_CookieStore):
    def __init__(self, root: Path) -> None:
        super().__init__(root, entropy=ENTROPY)
