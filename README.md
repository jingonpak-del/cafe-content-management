# 카페 게시글 감시 시스템

네이버 카페 게시글을 실시간 크롤링하여 **제목변경·삭제·잠복 홍보**를 추적하고,
업체(구글시트 등록)와 작성자 단위로 게시글 상태를 관리하는 카페 운영 도구.

## 핵심 기능

- **실시간 전체글 크롤링** — 등록 카페의 전체글(menu 0)을 라운드로빈으로 수집
- **상태 이력 추적** — 작성 → 제목변경 → 삭제 타임라인 (정상/제목변경/삭제/변경후삭제)
- **차등 재방문** — 일반글 10일(6회) / 업체글 30일(12회) 촘촘 추적
- **키워드 감시** — 제목·본문·댓글에서 금지 키워드 탐지
- **업체 매칭** — 구글시트(ID↔업체명) 기반, 일반등급 위장 업체까지 포착
- **멤버태그 저장** — 삭제·닉네임변경 후에도 작성자 역추적 가능
- **글관리** — 특정 작성자(아이디/닉네임/멤버코드) 등록 후 게시글 상태 추적

## 구조

```
src/
├── cafe_api.py        # 네이버 카페 API (목록/본문/댓글/회원관리)
├── session.py         # 네이버 로그인 세션 (웹 2단계 캡처)
├── cookie_store.py    # 쿠키 암호화 저장 (DPAPI)
├── db.py              # SQLite 스키마 + 조회
├── watcher.py         # 실시간 크롤링 엔진 (다중 카페)
├── keyword_monitor.py # 키워드 탐지
├── company_matcher.py # 구글시트 동기화 + 회원 해석
├── server.py          # FastAPI 서버 + WebSocket
└── static/index.html  # 대시보드 UI
```

## 실행

```bash
pip install -r requirements.txt
# config/settings.example.json 을 참고해 config/settings.json 작성
python -m src.server --port 8010
```

- `백그라운드_시작.vbs` — 창 없이 백그라운드 실행
- `서버_종료.bat` — 종료

접속: `http://localhost:8010`

## 매칭 원리

네이버는 실제 로그인 ID를 마스킹(`abc****`)하지만, 카페 매니저 권한의 회원관리
API(`ManageMemberListViewAjax`)로 **아이디→memberKey/멤버태그**를 조회할 수 있다.
게시글의 `writer.memberKey`와 이를 대조하여 업체·작성자를 식별한다.
(memberKey·멤버태그는 카페별로 다르므로 `(cafe_id, member_key)` 단위로 관리)

## 주의

- 로그인 세션(쿠키), DB, SSO 키는 `.gitignore`로 커밋 제외
- 카페 매니저/스탭 계정으로 로그인해야 회원 조회 가능
