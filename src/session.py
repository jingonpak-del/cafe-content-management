from __future__ import annotations

from pathlib import Path

# 만료·지문 판정은 공용 코어로 이관됐다(navercafe_core.sessioninfo).
# VerifyResult가 (ok, reason) 뒤에 기본값 필드를 더 갖게 되지만 기존 호출부는 그대로 동작한다.
# 이제 세션이 며칠 남았는지(days_left)와 곧 끊기는지(expiring)를 알 수 있다.
from navercafe_core.sessioninfo import (
    AUTH_COOKIES,
    EXPIRY_WARN_DAYS,
    VerifyResult,
    auth_expiry,
    auth_fingerprint,
    auth_token,
    days_left,
    describe as _describe,
    has_login_cookies,
    merge_expiry,
)

from .cookie_store import CookieStore
from . import cafe_api

NAVER_LOGIN = "https://nid.naver.com/nidlogin.login"
NAVER_HOME = "https://www.naver.com/"


def _make_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    options = Options()
    options.add_argument("--lang=ko-KR")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    return webdriver.Chrome(options=options)


class SessionManager:
    def __init__(self, session_dir: Path) -> None:
        self.store = CookieStore(session_dir)
        # 웹 기반 2단계 캡처용 — 열려있는 드라이버를 보관.
        self._pending_driver = None
        self._pending_account = None

    # ── 터미널용 (CLI) 1단계 캡처 ──────────────────────────────────────────
    def capture(self, account_id: str) -> Path:
        driver = _make_driver()
        try:
            driver.get(NAVER_LOGIN)
            input("브라우저에서 직접 로그인(캡챠/보호조치 통과)한 뒤, 여기서 Enter: ")
            driver.get(NAVER_HOME)
            cookies = driver.get_cookies()
            if not cookies:
                raise RuntimeError("쿠키를 가져오지 못했습니다.")
            return self.store.save(account_id, cookies)
        finally:
            driver.quit()

    # ── 웹용 2단계 캡처 ────────────────────────────────────────────────────
    def start_capture(self, account_id: str) -> None:
        """브라우저를 띄우고 네이버 로그인 페이지로 이동. 사용자가 직접 로그인.
        기존에 열려있던 드라이버가 있으면 정리 후 새로 연다."""
        self._cleanup_driver()
        driver = _make_driver()
        driver.get(NAVER_LOGIN)
        self._pending_driver = driver
        self._pending_account = account_id

    def finish_capture(self) -> VerifyResult:
        """열려있는 브라우저에서 쿠키를 캡처해 저장하고 브라우저를 닫는다."""
        if not self._pending_driver:
            return VerifyResult(False, "진행 중인 로그인 캡처가 없습니다. 먼저 '로그인 창 열기'를 누르세요.")
        driver = self._pending_driver
        account_id = self._pending_account
        try:
            driver.get(NAVER_HOME)
            cookies = driver.get_cookies()
            if not cookies:
                return VerifyResult(False, "쿠키를 가져오지 못했습니다. 로그인이 완료됐는지 확인하세요.")
            names = {c["name"] for c in cookies}
            if "NID_AUT" not in names or "NID_SES" not in names:
                return VerifyResult(False, "로그인이 완료되지 않았습니다 (로그인 쿠키 없음). 네이버에 로그인 후 다시 시도하세요.")
            self.store.save(account_id, cookies)
            return VerifyResult(True, f"{account_id} 로그인 세션 저장됨 (쿠키 {len(cookies)}개)")
        finally:
            self._cleanup_driver()

    def _cleanup_driver(self):
        if self._pending_driver:
            try:
                self._pending_driver.quit()
            except Exception:
                pass
        self._pending_driver = None
        self._pending_account = None

    def has_pending_capture(self) -> bool:
        return self._pending_driver is not None

    # ── 조회/검증 ─────────────────────────────────────────────────────────
    def load_cookies(self, account_id: str) -> list[dict]:
        return self.store.load(account_id).cookies

    def verify(self, account_id: str) -> VerifyResult:
        """쿠키 존재 + 로그인 쿠키 유무 + 만료 잔여일.

        `ok`의 뜻은 예전과 같다 — "쿠키를 실어서 써볼 만한가". 만료가 지났다고 False로
        내리지 않는다(네이버는 명목 만료가 지난 쿠키도 서버 세션이 살아 있으면 받아준다).
        만료는 `expiring`/`days_left`로 따로 알린다.
        """
        if not self.store.exists(account_id):
            return VerifyResult(False, "저장된 세션 없음")
        return _describe(self.store.load(account_id).cookies)

    def persist(self, account_id: str, client, *, prev: str | None = None) -> str | None:
        """네이버가 갱신한 쿠키를 세션 파일에 되쓴다.

        네이버는 인증 요청을 받으면 NID_AUT의 값은 그대로 두고 만료만 +30일로 다시 민다
        (실측). 그 갱신분이 메모리에만 남고 파일로 안 돌아가면, 프로세스를 재시작하는
        순간 처음 캡처한 낡은 쿠키로 되돌아간다. 이 메서드가 그걸 막는다.

        반환값은 저장한 지문. 저장을 건너뛴 경우(로그인 쿠키 없음 / 갱신 없음) None.
        """
        cookies = cafe_api.dump_cookies(client)
        if not has_login_cookies(cookies):
            return None          # 로그아웃 상태 쿠키로 멀쩡한 파일을 덮어쓰지 않는다
        try:
            cookies = merge_expiry(cookies, self.store.load(account_id).cookies)
        except Exception:
            pass
        fp = auth_fingerprint(cookies)
        if prev is not None and fp == prev:
            return None
        self.store.save(account_id, cookies)
        return fp

    def verify_live(self, account_id: str) -> VerifyResult:
        """실제 네이버에 요청을 보내 세션이 살아있는지(만료 여부) 확인."""
        vr = self.verify(account_id)
        if not vr.ok:
            return vr
        cookies = self.store.load(account_id).cookies
        client = cafe_api.make_client(cookies)
        try:
            if cafe_api.check_login(client):
                return VerifyResult(True, "로그인 세션 유효")
            return VerifyResult(False, "세션 만료됨 — 재로그인 필요")
        finally:
            client.close()

    def list_accounts(self) -> list[str]:
        return self.store.list_accounts()
