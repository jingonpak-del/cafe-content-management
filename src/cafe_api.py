"""Naver Cafe API client.

Endpoint:
    GET https://apis.naver.com/cafe-web/cafe2/ArticleListV2.json
        ?search.clubid={clubId}
        &search.queryType=lastArticle
        &search.menuid={menuId}      # 0 = whole cafe
        &search.page={page}
        &search.perPage={n}
"""
from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from typing import Any

import httpx
from bs4 import BeautifulSoup
from navercafe_core import browserenv

# UA는 공용 코어가 단일 출처를 관리한다(아이디관리가 설치된 크롬에서 뽑아 기록).
# 로그인은 실제 크롬으로 하고 크롤링은 여기서 HTTP로 하는데, 둘이 다른 Chrome 버전을
# 주장하면 같은 세션이 두 개의 브라우저인 척하는 셈이 된다.
UA = browserenv.load_ua()


def dump_cookies(client: httpx.Client) -> list[dict[str, Any]]:
    """네이버가 롤링 갱신한 쿠키를 CookieStore 저장 포맷으로 뽑는다(session.persist용)."""
    return browserenv.dump_cookies(client)
ARTICLE_LIST_URL = "https://apis.naver.com/cafe-web/cafe2/ArticleListV2.json"
# 전체글(menu 0) 지원 — 비로그인으로도 카페 전체 최신글 조회 가능.
BOARDLIST_URL = "https://apis.naver.com/cafe-web/cafe-boardlist-api/v1/cafes/{cafe_id}/menus/{menu_id}/articles"
ARTICLE_GW = "https://article.cafe.naver.com/gw"
GW_HEADERS = {"Referer": "https://cafe.naver.com/", "Origin": "https://cafe.naver.com"}
# 회원 1명이 카페에 쓴 "전체 작성글"(과거 포함). 스탭 세션 필요.
# ca-fe SPA 번들의 getMemberArticles(변수 ms) 역추적으로 확정. 회원단위 필터 정확,
# 페이지네이션 진짜(클램프 없음), 응답에 totalCount 포함. 필드 소문자 스키마:
# articleid/subject/writernickname/writerMemberKey/readcount/commentcount/upcount(=좋아요)/writeDateTimestamp.
MEMBER_ARTICLES_URL = "https://apis.naver.com/cafe-web/cafe-mobile/CafeMemberNetworkArticleListV3"
LOGIN_CHECK_URL = "https://apis.naver.com/cafe-home-web/cafe-home/v1/member/identifier"
MEMBER_PROFILE_URL = "https://apis.naver.com/cafe-web/cafe2/CafeMemberProfile.json"


class ArticleGoneError(RuntimeError):
    pass


def check_login(client: httpx.Client) -> bool:
    try:
        r = client.get(LOGIN_CHECK_URL, headers={"Referer": "https://cafe.naver.com/"}, timeout=8)
        return bool(r.json().get("message", {}).get("result", {}).get("loggedIn"))
    except Exception:
        return False


# ── 회원관리 (매니저 권한 필요) ──────────────────────────────────────────────
# ManageMemberListViewAjax.nhn: 아이디/닉네임/멤버태그로 회원 검색.
# 네이버가 실제 로그인 ID는 마스킹(didl****)하지만, member_key/member_tag는 제공.
# member_key/member_tag는 카페별로 다르므로 (cafe_id, ...) 단위로 관리해야 함.
MANAGE_MEMBER_URL = "https://cafe.naver.com/ManageMemberListViewAjax.nhn"

# 검색 대상 (search.basic.idType)
IDTYPE_ID = 0        # 아이디
IDTYPE_NICK = 1      # 별명(닉네임)
IDTYPE_TAG = 3       # 멤버태그

# 회원 등급 필터 (search.memberLevel) — 카페마다 값이 다를 수 있음
MEMBERLEVEL_PARTNER = 150   # 마산맘 기준 '상생업체/기관'


@dataclass(frozen=True)
class CafeMember:
    cafe_id: int
    member_key: str
    member_tag: str
    nickname: str
    masked_id: str          # 마스킹된 아이디 (didl****)
    level_name: str         # 등급명 (공주렐라 등)
    manager: bool
    staff: bool

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


def _manage_headers(cafe_id: int) -> dict[str, str]:
    return {
        "Referer": f"https://cafe.naver.com/ManageWholeMember.nhn?clubid={cafe_id}",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }


def _parse_member(cafe_id: int, m: dict[str, Any]) -> CafeMember:
    import html as _html
    return CafeMember(
        cafe_id=cafe_id,
        member_key=m.get("memberKey", ""),
        member_tag=m.get("memberTag", ""),
        nickname=m.get("nickname", ""),
        masked_id=m.get("maskedMemberId", ""),
        level_name=_html.unescape(m.get("memberLevelName", "") or ""),
        manager=bool(m.get("manager", False)),
        staff=bool(m.get("staff", False)),
    )


# ── 카페 스튜디오 회원관리 (2026-10-28 네이버 이전) ──────────────────────────
# 옛 ManageMemberListViewAjax.nhn 은 CAFE_STUDIO_MOVED 를 돌려준다. 새 API 는
# 운영진 '본인 인증(pii-auth)' 이 된 세션에서만 회원 목록을 준다(미인증 시 403 0040116).
STUDIO_API = "https://apis.cafe.naver.com/cafe-manage/cafe-manage/v1"
STUDIO_SEARCH_TYPE = {IDTYPE_ID: "MEMBER_ID", IDTYPE_NICK: "NICKNAME", IDTYPE_TAG: "MEMBER_TAG"}
#: 마지막 회원검색 시 본인인증 필요 여부 (대시보드 경고·오류 문구용)
STUDIO_STATE: dict[str, Any] = {"need_auth": False, "at": 0.0, "message": ""}


class StudioAuthRequired(RuntimeError):
    """카페 스튜디오 본인 인증이 안 된 세션 — 회원검색 불가(미가입으로 기록하면 안 됨)."""


def _studio_headers() -> dict[str, str]:
    return {"Referer": "https://studio.cafe.naver.com/", "Origin": "https://studio.cafe.naver.com"}


def studio_auth_status(client: httpx.Client) -> str:
    """VERIFIED | NOT_VERIFIED | ? — 카페 스튜디오 본인 인증 상태."""
    try:
        r = client.get(f"{STUDIO_API}/pii-auth/status", headers=_studio_headers(), timeout=10)
        return (r.json() or {}).get("status") or "?"
    except Exception:
        return "?"


def _parse_studio_member(cafe_id: int, m: dict[str, Any]) -> CafeMember:
    import html as _html
    lvl = m.get("memberLevel")
    lvl_name = m.get("memberLevelName") or (lvl.get("name") if isinstance(lvl, dict) else "") or ""
    return CafeMember(
        cafe_id=cafe_id,
        member_key=m.get("memberKey") or m.get("memberkey") or "",
        member_tag=m.get("memberTag") or "",
        nickname=m.get("nickname") or m.get("nickName") or "",
        masked_id=m.get("maskedMemberId") or m.get("maskId") or m.get("memberId") or "",
        level_name=_html.unescape(lvl_name),
        manager=bool(m.get("isManager") or m.get("isViceManager") or m.get("manager", False)),
        staff=bool(m.get("isStaff") or m.get("staff", False)),
    )


def search_members(
    cafe_id: int, keyword: str, id_type: int = IDTYPE_ID,
    per_page: int = 10, page: int = 1, client: httpx.Client | None = None,
) -> list[CafeMember]:
    """회원 검색. id_type: 0=아이디, 1=닉네임, 3=멤버태그. (카페 스튜디오 API)
    본인 인증이 안 된 세션이면 StudioAuthRequired 를 던진다 — 호출 측은 이를
    '회원 없음'으로 처리하면 안 된다."""
    import time as _t
    own = client is None
    client = client or make_client()
    try:
        params = {"page": page, "perPage": 30 if per_page <= 30 else 50,
                  "activityStopFilter": "ALL",
                  "searchType": STUDIO_SEARCH_TYPE.get(id_type, "NICKNAME"), "keyword": keyword}
        r = client.get(f"{STUDIO_API}/cafes/{cafe_id}/members/list", params=params,
                       headers=_studio_headers(), timeout=15)
        try:
            j = r.json() or {}
        except Exception:
            j = {}
        if r.status_code in (401, 403):
            msg = j.get("detail") or "카페 스튜디오 본인 인증이 필요합니다"
            STUDIO_STATE.update(need_auth=True, at=_t.time(), message=msg)
            raise StudioAuthRequired(msg)
        r.raise_for_status()
        STUDIO_STATE.update(need_auth=False, at=_t.time(), message="")
        data = j.get("result", j)
        rows = data.get("members") or data.get("memberList") or data.get("items") or []
        return [_parse_studio_member(cafe_id, m) for m in rows]
    finally:
        if own:
            client.close()


def find_member_by_id(cafe_id: int, naver_id: str, client: httpx.Client) -> CafeMember | None:
    """네이버 아이디(전체)로 특정 회원 1명을 찾음. 미가입/미검색 시 None."""
    members = search_members(cafe_id, naver_id, id_type=IDTYPE_ID, per_page=10, client=client)
    # 아이디 검색은 정확 매칭이지만, 안전하게 masked_id 앞부분 대조
    for m in members:
        return m   # 아이디 검색은 보통 정확히 1명
    return None


def resolve_member_by_key(cafe_id: int, member_key: str, nickname: str,
                          client: httpx.Client) -> CafeMember | None:
    """작성자 member_key로 회원 정보(멤버태그 등)를 조회.
    회원관리 API는 member_key 검색을 지원하지 않으므로, 닉네임으로 검색한 뒤
    member_key가 일치하는 회원을 골라낸다. (닉네임 중복 대비 member_key로 확정)"""
    if not nickname:
        return None
    try:
        members = search_members(cafe_id, nickname, id_type=IDTYPE_NICK, per_page=30, client=client)
    except StudioAuthRequired:
        raise
    except Exception:
        return None
    for m in members:
        if m.member_key == member_key:
            return m
    return None


def fetch_members_by_level(
    cafe_id: int, member_level: int, per_page: int = 50, page: int = 1,
    client: httpx.Client | None = None,
) -> tuple[list[CafeMember], int]:
    """특정 등급 회원 목록 + 전체 수. (예: 상생업체/기관=150)"""
    own = client is None
    client = client or make_client()
    try:
        data = {
            "search.clubid": cafe_id,
            "search.searchType": 0,
            "search.memberLevel": member_level,
            "search.perPage": per_page,
            "search.page": page,
            "search.sortType": 0,
            "search.sortOrder": 0,
            "search.paginationCached": "false",
            "search.totalCountCached": 0,
        }
        r = client.post(MANAGE_MEMBER_URL, data=data, headers=_manage_headers(cafe_id), timeout=15)
        result = r.json().get("result", {})
        total = result.get("pageOption", {}).get("totalCount", 0)
        members = [_parse_member(cafe_id, m) for m in result.get("members", [])]
        return members, total
    finally:
        if own:
            client.close()


@dataclass(frozen=True)
class ArticleSummary:
    cafe_id: int
    menu_id: int
    menu_name: str
    article_id: int
    title: str
    writer_nickname: str
    member_key: str
    read_count: int
    comment_count: int
    like_count: int
    write_ts: int
    last_comment_ts: int
    is_notice: bool
    blinded: bool
    has_image: bool
    summary: str = ""          # 목록 API의 본문 앞부분 (본문 수정 감지용)

    @property
    def url(self) -> str:
        return f"https://cafe.naver.com/ca-fe/cafes/{self.cafe_id}/articles/{self.article_id}"

    def to_row(self) -> dict[str, Any]:
        d = asdict(self)
        d["url"] = self.url
        return d


@dataclass(frozen=True)
class ArticleBody:
    cafe_id: int
    article_id: int
    menu_id: int
    title: str
    writer_nickname: str
    member_key: str
    write_ts: int
    read_count: int
    comment_count: int
    is_notice: bool
    content_html: str
    content_text: str


@dataclass(frozen=True)
class Comment:
    comment_id: int
    ref_id: int
    writer_nickname: str
    member_key: str
    content: str
    update_ts: int
    is_reply: bool
    is_deleted: bool
    is_article_writer: bool
    is_best: bool


def make_client(cookies: list[dict[str, Any]] | None = None) -> httpx.Client:
    jar = httpx.Cookies()
    for c in cookies or []:
        try:
            jar.set(c["name"], c["value"], domain=c.get("domain", ".naver.com"), path=c.get("path", "/"))
        except Exception:
            continue
    return httpx.Client(
        headers={"User-Agent": UA, "Accept": "application/json, text/plain, */*"},
        cookies=jar, timeout=12.0, follow_redirects=True,
    )


def resolve_club_id(cluburl: str, client: httpx.Client | None = None) -> int:
    own = client is None
    client = client or make_client()
    try:
        r = client.get(f"https://cafe.naver.com/{cluburl}", headers={"Referer": "https://cafe.naver.com/"})
        for pat in (r"clubid\D{0,4}(\d{4,})", r"cafeId\D{0,4}(\d{4,})", r"g_sClubId\D{0,4}(\d{4,})"):
            m = re.search(pat, r.text, re.I)
            if m:
                return int(m.group(1))
        raise RuntimeError(f"clubId를 찾을 수 없음: '{cluburl}'")
    finally:
        if own:
            client.close()


def fetch_article_list(
    club_id: int, menu_id: int = 0, per_page: int = 50, page: int = 1,
    client: httpx.Client | None = None,
) -> list[ArticleSummary]:
    """최신 게시글 목록.

    menu_id=0(전체글)은 ArticleListV2가 지원하지 않으므로(500) boardlist API를
    사용하고, 특정 게시판(menu_id>0)은 기존 ArticleListV2를 사용한다. 둘 다 비로그인
    조회가 가능하나, 회원전용 게시판은 로그인 세션이 필요할 수 있다.
    """
    if menu_id == 0:
        return _fetch_boardlist(club_id, menu_id, per_page, page, client)

    own = client is None
    client = client or make_client()
    try:
        params = {
            "search.clubid": club_id,
            "search.queryType": "lastArticle",
            "search.menuid": menu_id,
            "search.page": page,
            "search.perPage": per_page,
        }
        r = client.get(ARTICLE_LIST_URL, params=params,
                       headers={"Referer": f"https://cafe.naver.com/{club_id}"})
        body = r.json()
        msg = body.get("message", {})
        if str(msg.get("status")) != "200":
            err = msg.get("error", {})
            raise RuntimeError(f"목록 조회 실패 (status={msg.get('status')}, {err.get('msg')})")
        return [_to_summary(a) for a in msg.get("result", {}).get("articleList", [])]
    finally:
        if own:
            client.close()


def _fetch_boardlist(
    club_id: int, menu_id: int, per_page: int, page: int,
    client: httpx.Client | None = None,
) -> list[ArticleSummary]:
    """cafe-boardlist-api — 전체글(menu 0) 최신순 조회. item으로 감싸진 스키마."""
    own = client is None
    client = client or make_client()
    try:
        url = BOARDLIST_URL.format(cafe_id=club_id, menu_id=menu_id)
        r = client.get(
            url,
            params={"page": page, "pageSize": per_page, "sortBy": "TIME"},
            headers={"Referer": f"https://cafe.naver.com/{club_id}"},
        )
        result = r.json().get("result", {})
        arts = result.get("articleList", [])
        out = []
        for a in arts:
            if a.get("type") != "ARTICLE":
                continue
            out.append(_to_summary_boardlist(a.get("item", {})))
        return out
    finally:
        if own:
            client.close()


def _to_summary_boardlist(it: dict[str, Any]) -> ArticleSummary:
    """boardlist API의 item 스키마 → ArticleSummary."""
    writer = it.get("writerInfo") or {}
    return ArticleSummary(
        cafe_id=it.get("cafeId", 0),
        menu_id=it.get("menuId", 0),
        menu_name=it.get("menuName", ""),
        article_id=it.get("articleId", 0),
        title=it.get("subject", ""),
        writer_nickname=writer.get("nickName", ""),
        member_key=writer.get("memberKey", ""),
        read_count=it.get("readCount", 0),
        comment_count=it.get("commentCount", 0),
        like_count=it.get("likeCount", 0),
        write_ts=it.get("writeDateTimestamp", 0),
        last_comment_ts=it.get("lastCommentDateTimestamp", 0),
        is_notice=bool(it.get("noticeArticle", False)),
        blinded=bool(it.get("blindArticle", False)),
        has_image=bool(it.get("hasImage", False)),
        summary=it.get("summary") or "",
    )


def _to_summary(a: dict[str, Any]) -> ArticleSummary:
    return ArticleSummary(
        cafe_id=a.get("cafeId", 0),
        menu_id=a.get("menuId", 0),
        menu_name=a.get("menuName", ""),
        article_id=a.get("articleId", 0),
        title=a.get("subject", ""),
        writer_nickname=a.get("writerNickname") or a.get("nickname", ""),
        member_key=a.get("memberKey", ""),
        read_count=a.get("readCount", 0),
        comment_count=a.get("commentCount", 0),
        like_count=a.get("likeItCount", 0),
        write_ts=a.get("writeDateTimestamp", 0),
        last_comment_ts=a.get("lastCommentedTimestamp", 0),
        is_notice=bool(a.get("noticeArticle", False)),
        blinded=bool(a.get("blindArticle", False)),
        has_image=bool(a.get("attachImage") or a.get("imageCount", 0)),
    )


def _member_article_to_summary(a: dict[str, Any], club_id: int) -> ArticleSummary:
    """CafeMemberNetworkArticleListV3 의 소문자 스키마 → ArticleSummary."""
    club_menu = a.get("clubMenu") if isinstance(a.get("clubMenu"), dict) else {}
    return ArticleSummary(
        cafe_id=a.get("clubid", club_id),
        menu_id=a.get("menuid", 0),
        menu_name=(club_menu or {}).get("menuname", "") or a.get("menuname", ""),
        article_id=a.get("articleid", 0),
        title=a.get("subject", ""),
        writer_nickname=a.get("writernickname", ""),
        member_key=a.get("writerMemberKey", ""),
        read_count=a.get("readcount", 0),
        comment_count=a.get("commentcount", 0),
        like_count=a.get("upcount", 0),
        write_ts=a.get("writeDateTimestamp", 0),
        last_comment_ts=a.get("lastcommentdate", 0),
        is_notice=False,
        blinded=bool(a.get("readableBlindArticle", False)),
        has_image=bool(a.get("attachimageyn", False)),
    )


def fetch_member_articles(
    club_id: int, member_key: str, per_page: int = 30, page: int = 1,
    client: httpx.Client | None = None,
) -> tuple[list[ArticleSummary], int]:
    """회원 1명이 이 카페에 쓴 글 한 페이지 + 전체건수(totalCount). 스탭 세션 필요."""
    own = client is None
    client = client or make_client()
    try:
        r = client.get(
            MEMBER_ARTICLES_URL,
            params={"search.cafeId": club_id, "search.memberKey": member_key,
                    "search.perPage": per_page, "search.page": page, "requestFrom": "A"},
            headers={"Referer": f"https://cafe.naver.com/ca-fe/cafes/{club_id}/members/{member_key}",
                     "X-Cafe-Product": "pc"},
        )
        msg = r.json().get("message", {})
        if str(msg.get("status")) != "200":
            err = msg.get("error", {})
            raise RuntimeError(
                f"회원글 조회 실패 (status={msg.get('status')}, code={err.get('code')}, msg={err.get('msg')}). "
                f"세션이 이 카페 스탭 권한인지, memberKey가 맞는지 확인하세요.")
        result = msg.get("result", {})
        arts = [_member_article_to_summary(a, club_id) for a in result.get("articleList", [])]
        return arts, int(result.get("totalCount", 0))
    finally:
        if own:
            client.close()


def iter_member_articles(
    club_id: int, member_key: str, client: httpx.Client, per_page: int = 50,
    max_pages: int = 400, delay_s: float = 0.15,
):
    """회원의 전체 작성글을 페이지네이션하며 (article, total) 순차 yield.
    새 articleId가 안 나오면 종료(중복 클램프 안전)."""
    import time as _t
    seen: set[int] = set()
    for page in range(1, max_pages + 1):
        arts, total = fetch_member_articles(club_id, member_key, per_page=per_page, page=page, client=client)
        if not arts:
            break
        new = [a for a in arts if a.article_id not in seen]
        if not new:
            break
        for a in new:
            seen.add(a.article_id)
            yield a, total
        if len(arts) < per_page:
            break
        if delay_s:
            _t.sleep(delay_s)


def fetch_article_body(
    club_id: int, article_id: int, menu_id: int = 0, client: httpx.Client | None = None,
) -> ArticleBody:
    own = client is None
    client = client or make_client()
    try:
        url = f"{ARTICLE_GW}/v4/cafes/{club_id}/articles/{article_id}"
        params = {"query": "", "menuId": menu_id, "boardType": "L",
                  "useCafeId": "true", "requestFrom": "A"}
        r = client.get(url, params=params, headers=GW_HEADERS)
        if r.status_code in (403, 404):
            raise ArticleGoneError(f"삭제/비공개 (article {article_id}, HTTP {r.status_code})")
        art = r.json().get("result", {}).get("article", {})
        if not art or art.get("isReadable") is False:
            raise ArticleGoneError(f"조회 불가 (article {article_id})")
        html = art.get("contentHtml", "") or ""
        return ArticleBody(
            cafe_id=club_id,
            article_id=art.get("id", article_id),
            menu_id=(art.get("menu") or {}).get("id", menu_id),
            title=art.get("subject", ""),
            writer_nickname=(art.get("writer") or {}).get("nick", ""),
            member_key=(art.get("writer") or {}).get("memberKey", ""),
            write_ts=art.get("writeDate", 0),
            read_count=art.get("readCount", 0),
            comment_count=art.get("commentCount", 0),
            is_notice=bool(art.get("isNotice", False)),
            content_html=html,
            content_text=html_to_text(html),
        )
    finally:
        if own:
            client.close()


def fetch_comments(
    club_id: int, article_id: int, client: httpx.Client | None = None, max_pages: int = 20,
) -> list[Comment]:
    own = client is None
    client = client or make_client()
    out: list[Comment] = []
    seen: set[int] = set()
    try:
        for page in range(1, max_pages + 1):
            url = f"{ARTICLE_GW}/v4/cafes/{club_id}/articles/{article_id}/comments/pages/{page}"
            r = client.get(url, params={"requestFrom": "A", "orderBy": "asc"}, headers=GW_HEADERS)
            comments = r.json().get("result", {}).get("comments", {})
            items = comments.get("items", []) if isinstance(comments, dict) else []
            new = [c for c in items if c.get("id") not in seen]
            if not new:
                break
            for c in new:
                w = c.get("writer") or {}
                seen.add(c.get("id", 0))
                out.append(Comment(
                    comment_id=c.get("id", 0),
                    ref_id=c.get("refId", 0),
                    writer_nickname=w.get("nick", ""),
                    member_key=w.get("memberKey", ""),
                    content=c.get("content", "") or "",
                    update_ts=c.get("updateDate", 0),
                    is_reply=c.get("id") != c.get("refId"),
                    is_deleted=bool(c.get("isDeleted", False)),
                    is_article_writer=bool(c.get("isArticleWriter", False)),
                    is_best=bool(c.get("bestComment", False)),
                ))
        return out
    finally:
        if own:
            client.close()


def html_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)


# ── 스탭 삭제글 목록 (ManageRemoveArticleList) ────────────────────────────────
# 작성자≠삭제자인 삭제글(=스탭/매니저가 지운 글) 이력. 스탭 세션 필요.
REMOVE_LIST_URL = "https://cafe.naver.com/ManageRemoveArticleList.nhn"
_REMOVE_RESTORE_RE = re.compile(r"Restore\|(\d+)\|(\d+)")
_MEMBER_CELL_RE = re.compile(r"^(.*?)\((\w+\*+)\)\s*(\w+)?$")


@dataclass(frozen=True)
class RemovedArticle:
    cafe_id: int
    article_id: int
    menu_id: int
    menu_name: str
    title: str
    writer_nick: str
    writer_masked_id: str
    writer_tag: str
    remover_nick: str
    remover_masked_id: str
    remover_tag: str
    removed_date: str      # 'YYYY.MM.DD.'


def _parse_member_cell(text: str) -> tuple[str, str, str]:
    """'닉네임(mask****) 멤버태그' → (닉, 마스킹id, 태그)."""
    text = (text or "").strip()
    m = _MEMBER_CELL_RE.match(text)
    if m:
        return m.group(1).strip(), m.group(2), (m.group(3) or "")
    return text, "", ""


def fetch_removed_articles(cafe_id: int, page: int = 1,
                           client: httpx.Client | None = None) -> list[RemovedArticle]:
    """스탭/매니저가 삭제한 게시글 목록 (한 페이지). 매니저 권한 세션 필요."""
    own = client is None
    client = client or make_client()
    try:
        r = client.get(REMOVE_LIST_URL,
                       params={"search.clubid": cafe_id, "search.page": page},
                       headers={"Referer": f"https://cafe.naver.com/ManageWholeMember.nhn?clubid={cafe_id}"},
                       timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        out: list[RemovedArticle] = []
        for tr in soup.find_all("tr"):
            m = _REMOVE_RESTORE_RE.search(str(tr))
            if not m:
                continue
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue
            menu_id, article_id = int(m.group(1)), int(m.group(2))
            menu_name = tds[0].get_text(" ", strip=True)
            title = tds[1].get_text(" ", strip=True)
            wn, wm, wt = _parse_member_cell(tds[2].get_text(" ", strip=True))
            rn, rm, rt = _parse_member_cell(tds[3].get_text(" ", strip=True))
            date = tds[4].get_text(" ", strip=True)
            out.append(RemovedArticle(
                cafe_id, article_id, menu_id, menu_name, title,
                wn, wm, wt, rn, rm, rt, date,
            ))
        return out
    finally:
        if own:
            client.close()


# ---------------------------------------------------------------------------
# 네이버 카페 통계도구 (ManageNaverStat) - 멤버 순위 공식 통계
# cafe.stat.naver.com/api/cafe/{cid}/rank/{metric}
#   metric: memberCreate(작성게시글수) memberComment(작성댓글수)
#           memberLike(보낸좋아요) memberLiked(받은좋아요)
#           memberCv(조회수) memberVisit(방문횟수)
# 응답: result.statData[0].data.rows.{v, cnt, rank, memberInfos[0]=[{userId,nickName,...}]}
# 삭제 포함 공식 누적치. 랭킹이라 월별 상위 30명만 반환됨.
# ---------------------------------------------------------------------------

STAT_METRICS = {
    "create": "memberCreate",     # 작성 게시글수
    "comment": "memberComment",   # 작성 댓글수
    "like_sent": "memberLike",    # 보낸 좋아요수
    "like_recv": "memberLiked",   # 받은 좋아요수
    "view": "memberCv",           # 조회수
    "visit": "memberVisit",       # 방문횟수
}


@dataclass(frozen=True)
class MemberStatRow:
    rank: int
    user_id: str        # 실제 네이버 로그인 아이디
    nickname: str
    level_name: str
    join_date: str
    id_no: str          # 카페 통계 내부 멤버코드
    count: int


def _month_range(year_month: str) -> tuple[str, str]:
    """'2026-07' -> ('2026-07-01', '2026-07-31')"""
    y, m = (int(x) for x in year_month.split("-")[:2])
    if m == 12:
        ny, nm = y + 1, 1
    else:
        ny, nm = y, m + 1
    import datetime as _dt
    last = (_dt.date(ny, nm, 1) - _dt.timedelta(days=1)).day
    return f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{last:02d}"


def fetch_member_stat(
    cafe_id: int, year_month: str, key: str,
    client: httpx.Client | None = None, size: int = 100,
) -> list[MemberStatRow]:
    """단일 지표(key in STAT_METRICS)의 월간 멤버 순위를 가져온다.
    size: 순위 상위 몇 명까지(네이버 '더보기' 최대 100)."""
    import urllib.parse
    metric = STAT_METRICS[key]
    own = client is None
    client = client or make_client()
    try:
        start, _end = _month_range(year_month)
        mid = urllib.parse.quote("멤버")
        url = (
            f"https://cafe.stat.naver.com/api/cafe/{cafe_id}/rank/{metric}"
            f"?service=CAFE&timeDimension=MONTH&startDate={start}"
            f"&memberId={mid}&exclude=member%2Cboard%2CdashBoard&size={size}"
        )
        headers = {
            "Referer": f"https://cafe.stat.naver.com/cafe/{cafe_id}/rank/member",
            "Origin": "https://cafe.stat.naver.com",
        }
        r = client.get(url, headers=headers)
        if r.status_code != 200:
            return []
        try:
            rows = r.json()["result"]["statData"][0]["data"]["rows"]
        except (KeyError, IndexError, TypeError, ValueError):
            return []
        cnt = rows.get("cnt") or []
        rank = rows.get("rank") or []
        infos = (rows.get("memberInfos") or [[]])[0]
        out: list[MemberStatRow] = []
        for i in range(len(cnt)):
            info = infos[i] if i < len(infos) else {}
            uid = (info or {}).get("userId")
            if not uid:
                continue
            out.append(MemberStatRow(
                rank=rank[i] if i < len(rank) else i + 1,
                user_id=uid,
                nickname=(info.get("nickName") or ""),
                level_name=(info.get("memberLevelName") or ""),
                join_date=(info.get("joinDate") or ""),
                id_no=(info.get("idNo") or ""),
                count=cnt[i] or 0,
            ))
        return out
    finally:
        if own:
            client.close()


def fetch_member_stats_all(
    cafe_id: int, year_month: str,
    client: httpx.Client | None = None, size: int = 100,
) -> dict[str, dict[str, Any]]:
    """6개 지표를 모두 가져와 user_id 기준으로 병합. (각 지표 상위 size명, 최대 100)
    반환: { user_id: {nickname, level_name, join_date, create, comment,
                      like_sent, like_recv, view, visit} }
    """
    own = client is None
    client = client or make_client()
    try:
        merged: dict[str, dict[str, Any]] = {}
        for key in STAT_METRICS:
            for row in fetch_member_stat(cafe_id, year_month, key, client=client, size=size):
                slot = merged.setdefault(row.user_id, {
                    "user_id": row.user_id, "nickname": row.nickname,
                    "level_name": row.level_name, "join_date": row.join_date,
                    "create": None, "comment": None, "like_sent": None,
                    "like_recv": None, "view": None, "visit": None,
                })
                slot[key] = row.count
                if row.nickname:
                    slot["nickname"] = row.nickname
        return merged
    finally:
        if own:
            client.close()
