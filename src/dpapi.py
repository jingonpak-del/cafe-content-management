"""공용 코어(navercafe_core.dpapi)로 이관됨. 이 파일은 얇은 shim이다.

★ entropy는 이 프로젝트 고유값이다. 인기글은 b"ingigeul-tracker"를 쓴다.
통일하면 기존 세션 파일을 못 읽게 되므로 절대 바꾸지 말 것.
호출부(`from .dpapi import protect, unprotect`)는 그대로 두면 된다.
"""
from __future__ import annotations

from navercafe_core.dpapi import DPAPIError, protect as _protect, unprotect as _unprotect

ENTROPY = b"cafegamsi-tracker"

__all__ = ["protect", "unprotect", "DPAPIError", "ENTROPY"]


def protect(data: bytes, entropy: bytes = ENTROPY) -> bytes:
    return _protect(data, entropy)


def unprotect(data: bytes, entropy: bytes = ENTROPY) -> bytes:
    return _unprotect(data, entropy)
