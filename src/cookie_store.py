from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .dpapi import protect, unprotect

SESSION_VERSION = 1


@dataclass(frozen=True)
class SessionRecord:
    account_id: str
    saved_at: str
    cookies: list[dict[str, Any]]


class CookieStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, account_id: str) -> Path:
        digest = hashlib.sha256(account_id.strip().lower().encode("utf-8")).hexdigest()[:24]
        return self.root / f"{digest}.session"

    def save(self, account_id: str, cookies: list[dict[str, Any]]) -> Path:
        record = {
            "version": SESSION_VERSION,
            "account_id": account_id,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "cookies": _normalize_cookies(cookies),
        }
        payload = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        path = self.path_for(account_id)
        path.write_bytes(protect(payload))
        return path

    def load(self, account_id: str) -> SessionRecord:
        payload = unprotect(self.path_for(account_id).read_bytes())
        record = json.loads(payload.decode("utf-8"))
        if record.get("version") != SESSION_VERSION:
            raise ValueError(f"Unsupported session version: {record.get('version')}")
        return SessionRecord(record["account_id"], record["saved_at"], record["cookies"])

    def exists(self, account_id: str) -> bool:
        return self.path_for(account_id).exists()

    def delete(self, account_id: str) -> bool:
        path = self.path_for(account_id)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_accounts(self) -> list[str]:
        return [p.stem for p in self.root.glob("*.session")]


def _normalize_cookies(cookies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in cookies:
        item: dict[str, Any] = {
            "name": c.get("name"),
            "value": c.get("value"),
            "domain": c.get("domain"),
            "path": c.get("path", "/"),
            "secure": bool(c.get("secure", False)),
            "httpOnly": bool(c.get("httpOnly", False)),
        }
        if c.get("expiry") is not None:
            item["expiry"] = int(c["expiry"])
        if item["name"] and item["value"] is not None and item["domain"]:
            out.append(item)
    return out
