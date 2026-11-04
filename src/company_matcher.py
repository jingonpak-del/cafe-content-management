"""업체 매칭 — 구글시트(ID↔업체명) 동기화 + 회원 해석(ID→memberKey).

흐름:
  1. sync_sheet()   : 구글시트 CSV를 증분으로 읽어 company_master에 저장, 신규 ID는 pending
  2. resolve_pending(): pending ID를 각 카페 회원관리 API로 조회 → member_key/tag/등급 확보
  3. 게시글 매칭은 db.get_company_by_member_key() 로 (watcher에서 호출)

member_key/member_tag는 카페별로 다르므로 (naver_id, cafe_id) 단위로 관리.
실제 네이버 ID는 마스킹되지만, 시트의 '전체 아이디'로 검색하면 정확히 매칭됨.
"""
from __future__ import annotations

import csv
import io

import httpx

from .db import Database
from . import cafe_api


def sheet_csv_url(spreadsheet_id: str, gid: str | int) -> str:
    return (f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
            f"/export?format=csv&gid={gid}")


def sync_sheet(
    db: Database,
    spreadsheet_id: str,
    gid: str | int,
    cafe_ids: list[int],
    id_col: int = 1,        # B열 = 네이버 ID
    company_col: int = 2,   # C열 = 업체명
    log=print,
) -> dict:
    """구글시트 CSV를 읽어 company_master에 증분 반영. 신규 ID는 조회 대기(pending) 등록."""
    url = sheet_csv_url(spreadsheet_id, gid)
    r = httpx.get(url, timeout=20, follow_redirects=True)
    if r.status_code != 200 or "text/csv" not in r.headers.get("content-type", ""):
        raise RuntimeError(f"시트 접근 실패 (HTTP {r.status_code}). 링크 공유(뷰어)를 확인하세요.")

    rows = list(csv.reader(io.StringIO(r.text)))
    new_ids = 0
    total_valid = 0
    for idx, row in enumerate(rows):
        if idx == 0:  # 헤더
            continue
        if len(row) <= max(id_col, company_col):
            continue
        naver_id = (row[id_col] or "").strip()
        company = (row[company_col] or "").strip()
        if not naver_id or not company:
            continue
        total_valid += 1
        is_new = db.upsert_company_master(naver_id, company, idx)
        db.enqueue_member_resolution(naver_id, company, cafe_ids)
        if is_new:
            new_ids += 1

    log(f"📋 시트 동기화: 유효 {total_valid}행, 신규 {new_ids}개 ID")
    return {"total": total_valid, "new": new_ids}


def resolve_pending(db: Database, client: httpx.Client, batch: int = 20,
                    min_gap_s: float = 0.4, log=print) -> int:
    """pending 상태 (naver_id, cafe_id)를 회원관리 API로 조회해 해석.
    아이디 검색으로 member_key/tag/등급 확보. 미가입은 not_member로 기록."""
    import time
    rows = db.get_pending_resolutions(limit=batch)
    if not rows:
        return 0
    done = 0
    for row in rows:
        naver_id = row["naver_id"]
        cafe_id = row["cafe_id"]
        company = row["company_name"]
        try:
            member = cafe_api.find_member_by_id(cafe_id, naver_id, client)
            if member and member.member_key:
                db.save_member_resolution(naver_id, cafe_id, member, company, "resolved")
            else:
                db.save_member_resolution(naver_id, cafe_id, None, company, "not_member")
            done += 1
        except cafe_api.StudioAuthRequired:
            # 카페 스튜디오 본인인증 전 — pending 그대로 두고 이번 배치 중단(미가입 기록 금지)
            break
        except Exception as e:
            log(f"  ! 해석 실패 {naver_id}@{cafe_id}: {e}")
        time.sleep(min_gap_s)
    return done
