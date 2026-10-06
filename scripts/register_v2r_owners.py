"""post.whitedr.com(autotool) 계정을 게시글감시 업체 아이디 리스트에 등록.
소유주 기준: owner_id=1 → '에코프라임', owner_id IS NULL → '우리'.
company_master 등록 + 두 카페 회원해석(member_key) → company_member(resolved).
이미 시트에 있는 아이디(예: tpdlqlal='타겟온유')는 덮어쓰지 않고 건너뜀."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db import Database
from src import cafe_api
from src.session import SessionManager

ROOT = Path(__file__).resolve().parents[1]
CAFES = [14793916, 21031223]

ECO = "anan2303,bkm31736,bsy11842,chocpo,dlgjswn728,jusic111,lacid7,twom-ent1,twom1203".split(",")
OURS = ("bajavkul,bakagyp8,blueday356,capri322,cc99m,chejincompany,cherd9,cwfreshtrend,"
        "dkstjdms2016,dltldbtldb34,dms1215b,dreb5167,dsw99244,eehsl789,estherscent,fame8770,"
        "fierce2962,fiqjnwqa18,flashes304,gkehdgns07,hadleyparsons,hospitalm,ibliever1,jayuin6,"
        "jericho288,joonsmart0717,kbdnkjong,khk722700,ldo40111,leontyson,lrajyykvv,mgavetilll,"
        "mjmj1006,mm00147,nada051,nijegat,nijybek3,nocisred,noon10345,nwj610pi,oblige14031,"
        "pcxtooiyv,pezodozf,phil46mark48,qesopmyp,qpdb5048,quddhrtodrkr,sharp8028,skfdksk2,"
        "smdia1212,ssung2i,syyoy3556,tempt5880,tlatkddk02,townuo36,tpdlqlal,vanezozj,vysk6713,"
        "watdoyumin,wayycqt,wjatns0531,wjdjdgdeqj,wkwh1277,yyyu2026").split(",")

GROUPS = [("에코프라임", ECO), ("우리", OURS)]


def main():
    import json
    cfg = json.load(open(ROOT / "config" / "settings.json", encoding="utf-8"))
    acc = cfg.get("account_id")
    sm = SessionManager(ROOT / "data" / "sessions")
    if not sm.verify(acc).ok:
        print("로그인 세션 없음:", acc); return
    client = cafe_api.make_client(sm.load_cookies(acc))
    db = Database(str(ROOT / "data" / "gamsi.db"))

    # 시트에 이미 있는 아이디는 보존
    have = {r[0]: r[1] for r in db.conn.execute("SELECT naver_id, company_name FROM company_master")}

    idx = 900000
    skipped, queued = [], 0
    for company, ids in GROUPS:
        for nid in ids:
            nid = nid.strip().lower()
            if not nid:
                continue
            if nid in have and have[nid] not in (company,):
                skipped.append((nid, have[nid])); continue
            db.upsert_company_master(nid, company, idx); idx += 1
            db.enqueue_member_resolution(nid, company, CAFES)
            queued += 1
    print(f"마스터 등록/큐: {queued}건, 보존(스킵): {skipped}")

    resolved = notm = 0
    for company, ids in GROUPS:
        for nid in ids:
            nid = nid.strip().lower()
            if not nid or (nid in have and have[nid] not in (company,)):
                continue
            for cid in CAFES:
                try:
                    m = cafe_api.find_member_by_id(cid, nid, client)
                except Exception as e:
                    print("  ! 검색오류", nid, cid, e); m = None
                if m and m.member_key:
                    db.save_member_resolution(nid, cid, m, company, "resolved")
                    resolved += 1
                    print(f"  ✓ [{company}] {nid}@{cid} -> {m.nickname}")
                else:
                    db.save_member_resolution(nid, cid, None, company, "not_member")
                    notm += 1
                time.sleep(0.3)
    print(f"완료: resolved {resolved} / not_member {notm}")
    # 요약
    for company, _ in GROUPS:
        n = db.conn.execute(
            "SELECT COUNT(*) FROM company_member WHERE company_name=? AND status='resolved' AND member_key IS NOT NULL",
            (company,)).fetchone()[0]
        print(f"  [{company}] 업체아이디 리스트 표시(카페×회원): {n}")


if __name__ == "__main__":
    main()
