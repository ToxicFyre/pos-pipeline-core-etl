"""Tests for CSRF token extraction and authentication-aware error handling."""

from __future__ import annotations

import os

os.environ["POS_BRONZE_BACKEND"] = "legacy_http"

from unittest.mock import MagicMock

import pytest
import requests

from pos_core.etl.raw.extraction import (
    REPORT_PAGE_PATH,
    get_csrf_from_html,
    require_csrf_token,
)
from pos_core.exceptions import AuthenticationError, CsrfTokenError

BASE_URL = "https://wansoft.example.com"
TEST_PASSWORD = "super-secret-password-12345"
TEST_CSRF = "test-token"
COOKIE_VALUE = "session-cookie-secret-xyz"


def _make_response(*, url: str, status_code: int = 200, text: str = "") -> requests.Response:
    resp = requests.Response()
    resp.status_code = status_code
    resp.url = url
    resp._content = text.encode("utf-8")
    resp.encoding = "utf-8"
    resp.history = []
    return resp


def _login_page_html() -> str:
    return (
        "<html><head><title>Login</title></head><body>"
        '<form action="/Account/LogOn" method="post">'
        '<input type="text" name="UserName">'
        '<input type="password" name="Password">'
        "</form></body></html>"
    )


def _authenticated_page_without_csrf() -> str:
    return (
        f"<html><head><title>Report</title></head><body>"
        f"<h1>Report at {REPORT_PAGE_PATH}</h1></body></html>"
    )


def _authenticated_page_with_csrf(token: str = TEST_CSRF) -> str:
    return (
        "<html><head><title>Report</title></head><body>"
        f'<input type="hidden" name="__RequestVerificationToken" value="{token}">'
        "</body></html>"
    )


class TestRequireCsrfToken:
    def test_login_page_without_csrf_raises_authentication_error(self) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []
        response = _make_response(
            url=f"{BASE_URL}/Account/LogOn",
            text=_login_page_html(),
        )

        with pytest.raises(AuthenticationError) as exc_info:
            require_csrf_token(
                None,
                context=f"Report page ({REPORT_PAGE_PATH})",
                response=response,
                session=session,
            )

        assert "login page" in str(exc_info.value).lower()
        assert "CSRF token is MANDATORY" not in str(exc_info.value)

    def test_authenticated_page_without_csrf_raises_csrf_error(self) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []
        response = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_authenticated_page_without_csrf(),
        )

        with pytest.raises(CsrfTokenError) as exc_info:
            require_csrf_token(
                None,
                context=f"Report page ({REPORT_PAGE_PATH})",
                response=response,
                session=session,
            )

        assert "CSRF token was not found on the authenticated report page" in str(exc_info.value)

    def test_valid_csrf_token_is_returned(self) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []
        response = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_authenticated_page_with_csrf(),
        )

        token = require_csrf_token(
            get_csrf_from_html(response.text),
            context=f"Report page ({REPORT_PAGE_PATH})",
            response=response,
            session=session,
        )

        assert token == TEST_CSRF

    def test_csrf_error_does_not_leak_token_or_password(self) -> None:
        session = MagicMock(spec=requests.Session)
        cookie = MagicMock()
        cookie.name = "ASP.NET_SessionId"
        cookie.value = COOKIE_VALUE
        session.cookies = [cookie]

        html = _authenticated_page_with_csrf(token=TEST_CSRF)
        response = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=html,
        )

        # Token present in HTML but passed as None to simulate parse failure
        with pytest.raises(CsrfTokenError) as exc_info:
            require_csrf_token(
                None,
                context=f"Report page ({REPORT_PAGE_PATH})",
                response=response,
                session=session,
            )

        error_text = str(exc_info.value)
        assert TEST_CSRF not in error_text
        assert TEST_PASSWORD not in error_text
        assert COOKIE_VALUE not in error_text
