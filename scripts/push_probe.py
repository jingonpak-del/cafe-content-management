"""폰 알림 탐침 (2026-11-09) — 카페앱 '게시판 새글 알림'이 감시 트리거로 쓸 만한지 재는 도구.

adb 로 폰의 알림 목록(dumpsys notification)을 1초마다 읽어, 지정한 앱(기본 카페앱)에서 새로 뜬 알림만
data/push_probe.jsonl 에 시각과 함께 남긴다. 다른 앱 알림은 읽어도 저장·출력하지 않는다.

  python scripts/push_probe.py --serial R3CR70R5AEA            # 카페앱 알림 감시
  python scripts/push_probe.py --serial R3CR70R5AEA --pkg com.android.shell   # 도구 시험용
  python scripts/push_probe.py --report                        # 알림 도착시각 vs 글 작성시각 지연 집계
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "push_probe.jsonl"
ADB = r"C:\adb\platform-tools\adb.exe"
CAFE_PKG = "com.nhn.android.navercafe"
REC_RE = re.compile(r"^\s*NotificationRecord\(0x[0-9a-f]+: pkg=(\S+) user=\S+ id=(-?\d+) tag=(\S*) importance=(\d+) key=(\S+)")
FIELD_RE = re.compile(r"android\.(title|text|bigText|subText)=\w*(?:Spannable)?\w* ?\((.*)\)\s*$")


def dump(serial: str) -> str:
    return subprocess.run([ADB, "-s", serial, "shell", "dumpsys notification --noredact"],
                          capture_output=True, timeout=20).stdout.decode("utf-8", "replace")


def records(text: str, pkg: str):
    """지정한 앱의 알림 레코드만 {key, id, tag, fields, raw} 로 뽑는다."""
    cur = None
    for line in text.splitlines():
        m = REC_RE.match(line)
        if m:
            if cur and cur["pkg"] == pkg:
                yield cur
            cur = {"pkg": m.group(1), "id": m.group(2), "tag": m.group(3), "key": m.group(5),
                   "fields": {}, "raw": [], "when": ""}
            continue
        if cur is None or cur["pkg"] != pkg:
            continue
        if line.startswith("    ") and not line.startswith("      ") and "NotificationRecord" not in line and cur["raw"]:
            pass
        f = FIELD_RE.search(line.strip())
        if f:
            cur["fields"][f.group(1)] = f.group(2)
        w = re.search(r"\bwhen=(\d{12,})", line)
        if w and not cur["when"]:
            cur["when"] = w.group(1)
        if "Push_Payload_Json=" in line:      # 네이버가 보낸 푸시 원문(글 번호·발송 시각 포함)
            m2 = re.search(r"Push_Payload_Json=String \((\{.*\})\)\s*$", line.strip())
            if m2:
                try:
                    cur["payload"] = json.loads(m2.group(1))
                except Exception:
                    pass
        # 알림을 눌렀을 때 열릴 주소(글 번호가 들어 있을 수 있다)
        if "contentIntent" in line or "dat=" in line or "articleid" in line.lower():
            cur["raw"].append(line.strip()[:300])
    if cur and cur["pkg"] == pkg:
        yield cur


def watch(serial: str, pkg: str):
    seen: set[str] = set()
    first = True
    print(f"[probe] {serial} · {pkg} 알림 감시 시작 — 중지는 Ctrl+C", flush=True)
    while True:
        t0 = time.time()
        try:
            text = dump(serial)
        except Exception as e:
            print("  ! adb 실패:", e, flush=True)
            time.sleep(3)
            continue
        for r in records(text, pkg):
            sig = hashlib.md5((r["key"] + r["when"] + json.dumps(r["fields"], ensure_ascii=False)).encode()).hexdigest()
            if sig in seen:
                continue
            seen.add(sig)
            if first:          # 시작 시점에 이미 떠 있던 알림은 기준만 잡는다
                continue
            row = {"seen_at": int(time.time() * 1000), "pkg": pkg, "key": r["key"], "when": r["when"],
                   **r["fields"], "payload": r.get("payload"), "intent": r["raw"][:3]}
            with OUT.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            print("  🔔", time.strftime("%H:%M:%S"), r["fields"].get("title", ""), "|", r["fields"].get("text", "")[:60], flush=True)
        first = False
        time.sleep(max(0.2, 1.0 - (time.time() - t0)))


def report():
    """푸시(messageKey)별로 글 작성 → 푸시 발송 → 폰 수신 → PC 포착 시각과, 우리 감시의 첫 발견 시각을 비교."""
    db = sqlite3.connect(ROOT / "data" / "gamsi.db")
    seen: dict[str, dict] = {}
    for l in OUT.read_text(encoding="utf-8").splitlines():
        if not l.strip():
            continue
        r = json.loads(l)
        pl = r.get("payload") or {}
        if not pl.get("articleId"):
            continue
        seen.setdefault(str(pl.get("messageKey")), {**pl, "seen_at": r["seen_at"]})
    rows = []
    for k, pl in seen.items():
        a = db.execute("SELECT write_ts, first_seen_at, title FROM articles WHERE cafe_id=? AND article_id=?",
                       (pl["cafeId"], pl["articleId"])).fetchone()
        if not a:
            continue
        ws, fs = a[0], a[1]
        rows.append((ws, (pl["sentAt"] - ws) / 1000, (pl["receivedAt"] - ws) / 1000,
                     (pl["seen_at"] - ws) / 1000, (fs - ws) / 1000 if fs else None, a[2]))
    rows.sort()
    print("작성시각   푸시발송 폰수신 PC포착 | 우리감시발견(초, 글 작성 기준)")
    for ws, snt, rcv, pc, our, t in rows:
        print(time.strftime("%m-%d %H:%M:%S", time.localtime(ws / 1000)),
              f"{snt:6.1f} {rcv:6.1f} {pc:6.1f} | {our if our is None else round(our, 1)}  {t[:16]}")
    if rows:
        med = lambda xs: sorted(xs)[len(xs) // 2]
        print(f"\n건수 {len(rows)} · 푸시발송 중앙값 {med([r[1] for r in rows]):.1f}초 · 폰수신 {med([r[2] for r in rows]):.1f}초 · "
              f"PC포착 {med([r[3] for r in rows]):.1f}초 · 우리감시 {med([r[4] for r in rows if r[4] is not None]):.1f}초")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--serial")
    ap.add_argument("--pkg", default=CAFE_PKG)
    ap.add_argument("--report", action="store_true")
    a = ap.parse_args()
    if a.report:
        report()
    elif a.serial:
        try:
            watch(a.serial, a.pkg)
        except KeyboardInterrupt:
            pass
    else:
        ap.error("--serial 또는 --report")
