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
                   **r["fields"], "intent": r["raw"][:3]}
            with OUT.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            print("  🔔", time.strftime("%H:%M:%S"), r["fields"].get("title", ""), "|", r["fields"].get("text", "")[:60], flush=True)
        first = False
        time.sleep(max(0.2, 1.0 - (time.time() - t0)))


def report():
    """알림이 글 작성 뒤 몇 초 만에 왔는지: 제목이 같은 등업글을 DB 에서 찾아 비교."""
    db = sqlite3.connect(ROOT / "data" / "gamsi.db")
    rows = [json.loads(l) for l in OUT.read_text(encoding="utf-8").splitlines() if l.strip()]
    lat = []
    for r in rows:
        txt = (r.get("text") or r.get("bigText") or "")
        hit = db.execute("SELECT title, write_ts FROM articles WHERE menu_id IN (1688,1106) AND write_ts>? "
                         "AND instr(?, substr(title,1,12))>0 ORDER BY write_ts DESC LIMIT 1",
                         (r["seen_at"] - 3600_000, txt + " " + (r.get("title") or ""))).fetchone()
        d = (r["seen_at"] - hit[1]) / 1000 if hit else None
        lat.append(d)
        print(time.strftime("%m-%d %H:%M:%S", time.localtime(r["seen_at"] / 1000)), r.get("title", "")[:20],
              "| 글→알림", f"{d:.0f}초" if d is not None else "(글 못 찾음)")
    ok = sorted(x for x in lat if x is not None)
    if ok:
        print(f"\n건수 {len(ok)} · 중앙값 {ok[len(ok)//2]:.0f}초 · 최대 {ok[-1]:.0f}초")


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
