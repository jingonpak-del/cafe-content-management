from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .cookie_store import CookieStore
from . import cafe_api

NAVER_LOGIN = "https://nid.naver.com/nidlogin.login"
NAVER_HOME = "https://www.naver.com/"


@dataclass(frozen=True)
class VerifyResult:
    ok: bool
    reason: str


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
        """쿠키 존재 + 로그인 쿠키(NID_AUT/NID_SES) 유무만 빠르게 확인."""
        if not self.store.exists(account_id):
            return VerifyResult(False, "저장된 세션 없음")
        cookies = self.store.load(account_id).cookies
        names = {c["name"] for c in cookies}
        if "NID_AUT" not in names or "NID_SES" not in names:
            return VerifyResult(False, "로그인 쿠키(NID_AUT/NID_SES) 없음 — 재로그인 필요")
        return VerifyResult(True, f"쿠키 {len(cookies)}개 보유")

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
