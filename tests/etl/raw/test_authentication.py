"""Tests for Wansoft authentication helpers and login_if_needed()."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests
from bs4 import BeautifulSoup

from pos_core.etl.raw.extraction import (
    INVENTORY_TRANSFERS_PAGE,
    REPORT_PAGE_PATH,
    _find_login_form,
    extract_login_validation_messages,
    is_login_response,
    login_if_needed,
    safe_response_diagnostics,
)
from pos_core.exceptions import AuthenticationError, AuthorizationError

BASE_URL = "https://wansoft.example.com"
TEST_PASSWORD = "super-secret-password-12345"
TEST_CSRF = "csrf-token-value-abc123"
COOKIE_VALUE = "session-cookie-secret-xyz"


def _make_response(
    *,
    url: str,
    status_code: int = 200,
    text: str = "",
    history: list[requests.Response] | None = None,
) -> requests.Response:
    resp = requests.Response()
    resp.status_code = status_code
    resp.url = url
    resp._content = text.encode("utf-8")
    resp.encoding = "utf-8"
    resp.history = history or []
    return resp


def _protected_page_html() -> str:
    return (
        "<html><head><title>Sales Report</title></head><body>"
        '<input type="hidden" name="__RequestVerificationToken" value="token">'
        "</body></html>"
    )


def _login_page_html(
    *,
    action: str = "/Account/LogOn",
    validation_message: str | None = None,
    extra_forms: str = "",
) -> str:
    validation_block = ""
    if validation_message:
        validation_block = (
            f'<div class="validation-summary-errors">{validation_message}</div>'
        )
    return (
        f"<html><head><title>Login</title></head><body>{validation_block}{extra_forms}"
        f'<form action="{action}" method="post">'
        '<input type="text" name="UserName" value="">'
        '<input type="password" name="Password" value="">'
        '<input type="hidden" name="ReturnUrl" value="">'
        "</form></body></html>"
    )


class TestIsLoginResponse:
    def test_final_url_contains_logon(self) -> None:
        resp = _make_response(
            url=f"{BASE_URL}/Account/LogOn",
            text=_login_page_html(),
        )
        assert is_login_response(resp) is True

    def test_redirect_history_to_logon(self) -> None:
        redirect = _make_response(
            url=f"{BASE_URL}/Reports/ConsolidatedSalesMasterReport",
            status_code=302,
        )
        redirect.headers["Location"] = "/Account/LogOn"
        resp = _make_response(
            url=f"{BASE_URL}/Account/LogOn",
            text=_login_page_html(),
            history=[redirect],
        )
        assert is_login_response(resp) is True

    def test_login_form_in_html(self) -> None:
        resp = _make_response(
            url=f"{BASE_URL}/some/page",
            text=_login_page_html(action="Account/LogOn"),
        )
        assert is_login_response(resp) is True

    def test_protected_page_is_not_login(self) -> None:
        resp = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_protected_page_html(),
        )
        assert is_login_response(resp) is False


class TestLoginFormDetection:
    def test_find_login_form_accepts_root_action_on_logon_url(self) -> None:
        """Regression: Wansoft login form posts to app root, not /Account/LogOn."""
        page_url = (
            f"{BASE_URL}/Account/LogOn?ReturnUrl=%2FReports%2FConsolidatedSalesMasterReport"
        )
        html = _login_page_html(action="/Wansoft.Web/")
        soup = BeautifulSoup(html, "html.parser")

        form = _find_login_form(soup, page_url)

        assert form is not None
        assert form.get("action") == "/Wansoft.Web/"

    def test_login_if_needed_posts_to_app_root_action(self) -> None:
        """Regression: login_if_needed must not fail when form action is app root."""
        session = MagicMock(spec=requests.Session)
        session.cookies = []

        login_html = _login_page_html(action="/Wansoft.Web/")
        login_page = _make_response(
            url=f"{BASE_URL}/Account/LogOn?ReturnUrl=%2FReports%2FConsolidatedSalesMasterReport",
            text=login_html,
        )
        protected = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_protected_page_html(),
        )

        get_calls = {"count": 0}

        def fake_get(url: str, **_kwargs: object) -> requests.Response:
            if url == f"{BASE_URL}/":
                return _make_response(url=f"{BASE_URL}/", status_code=200)
            get_calls["count"] += 1
            if get_calls["count"] == 1:
                return login_page
            return protected

        session.get.side_effect = fake_get
        session.post.return_value = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_protected_page_html(),
        )

        login_if_needed(session, BASE_URL, "user", TEST_PASSWORD)

        post_args = session.post.call_args
        posted_url = post_args[0][0]
        posted_fields = post_args[1]["data"]
        assert posted_url == f"{BASE_URL}/Wansoft.Web/"
        assert posted_fields["UserName"] == "user"
        assert posted_fields["Password"] == TEST_PASSWORD


class TestExtractLoginValidationMessages:
    def test_extracts_validation_summary(self) -> None:
        html = _login_page_html(validation_message="Usuario o contraseña incorrectos.")
        messages = extract_login_validation_messages(html)
        assert messages == ["Usuario o contraseña incorrectos."]

    def test_deduplicates_and_skips_empty(self) -> None:
        html = (
            '<div class="validation-summary-errors">Bad credentials</div>'
            '<div class="field-validation-error">Bad credentials</div>'
            '<div class="alert-danger"></div>'
        )
        messages = extract_login_validation_messages(html)
        assert messages == ["Bad credentials"]


class TestLoginIfNeeded:
    def test_successful_existing_session(self, caplog: pytest.LogCaptureFixture) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []

        protected = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_protected_page_html(),
        )

        def fake_get(url: str, **_kwargs: object) -> requests.Response:
            if url == f"{BASE_URL}/":
                return _make_response(url=f"{BASE_URL}/", status_code=200)
            if url == f"{BASE_URL}{REPORT_PAGE_PATH}":
                return protected
            raise AssertionError(f"Unexpected GET: {url}")

        session.get.side_effect = fake_get

        with caplog.at_level("INFO"):
            login_if_needed(session, BASE_URL, "user", TEST_PASSWORD)

        session.post.assert_not_called()
        assert "Existing Wansoft session is authenticated" in caplog.text
        assert "Login succeeded" not in caplog.text

    def test_invalid_password_returns_login_page(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []

        login_page = _make_response(
            url=f"{BASE_URL}/Account/LogOn",
            text=_login_page_html(),
        )
        post_response = _make_response(
            url=f"{BASE_URL}/Account/LogOn",
            text=_login_page_html(),
        )

        def fake_get(url: str, **_kwargs: object) -> requests.Response:
            if url == f"{BASE_URL}/":
                return _make_response(url=f"{BASE_URL}/", status_code=200)
            return login_page

        session.get.side_effect = fake_get
        session.post.return_value = post_response

        with caplog.at_level("INFO"), pytest.raises(AuthenticationError) as exc_info:
            login_if_needed(session, BASE_URL, "user", TEST_PASSWORD)

        assert "WS_USER and WS_PASS" in str(exc_info.value)
        assert "Login succeeded" not in caplog.text
        assert "CSRF" not in str(exc_info.value)

    def test_login_validation_message_in_error(self) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []

        login_html = _login_page_html(
            validation_message="Usuario o contraseña incorrectos."
        )
        login_page = _make_response(url=f"{BASE_URL}/Account/LogOn", text=login_html)

        session.get.side_effect = lambda url, **_kwargs: (
            _make_response(url=f"{BASE_URL}/", status_code=200)
            if url == f"{BASE_URL}/"
            else login_page
        )
        session.post.return_value = login_page

        with pytest.raises(AuthenticationError) as exc_info:
            login_if_needed(session, BASE_URL, "user", TEST_PASSWORD)

        assert "Usuario o contraseña incorrectos." in str(exc_info.value)

    def test_missing_credentials(self) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []
        login_page = _make_response(
            url=f"{BASE_URL}/Account/LogOn",
            text=_login_page_html(),
        )

        session.get.side_effect = lambda url, **_kwargs: (
            _make_response(url=f"{BASE_URL}/", status_code=200)
            if url == f"{BASE_URL}/"
            else login_page
        )

        with patch.dict("os.environ", {}, clear=True), pytest.raises(AuthenticationError) as exc_info:
            login_if_needed(session, BASE_URL, None, None)

        assert "WS_USER" in str(exc_info.value) or "credentials" in str(exc_info.value).lower()

    def test_unauthorized_protected_page(self) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []

        login_page = _make_response(
            url=f"{BASE_URL}/Account/LogOn",
            text=_login_page_html(),
        )
        forbidden = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            status_code=403,
            text="Forbidden",
        )

        get_calls = {"count": 0}

        def fake_get(url: str, **_kwargs: object) -> requests.Response:
            if url == f"{BASE_URL}/":
                return _make_response(url=f"{BASE_URL}/", status_code=200)
            get_calls["count"] += 1
            if get_calls["count"] == 1:
                return login_page
            return forbidden

        session.get.side_effect = fake_get
        session.post.return_value = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_protected_page_html(),
        )

        with pytest.raises(AuthorizationError) as exc_info:
            login_if_needed(session, BASE_URL, "user", TEST_PASSWORD)

        assert REPORT_PAGE_PATH in str(exc_info.value)

    def test_relative_form_action_uses_urljoin(self) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []

        login_html = _login_page_html(action="Account/LogOn")
        login_page = _make_response(
            url=f"{BASE_URL}/",
            text=login_html,
        )
        protected = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_protected_page_html(),
        )

        get_calls = {"count": 0}

        def fake_get(url: str, **_kwargs: object) -> requests.Response:
            if url == f"{BASE_URL}/":
                return _make_response(url=f"{BASE_URL}/", status_code=200)
            get_calls["count"] += 1
            if get_calls["count"] == 1:
                return login_page
            return protected

        session.get.side_effect = fake_get
        session.post.return_value = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_protected_page_html(),
        )

        login_if_needed(session, BASE_URL, "user", TEST_PASSWORD)

        post_args = session.post.call_args
        posted_url = post_args[0][0]
        assert posted_url == f"{BASE_URL}/Account/LogOn"

    def test_verification_path_for_transfers(self, caplog: pytest.LogCaptureFixture) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []

        protected = _make_response(
            url=f"{BASE_URL}{INVENTORY_TRANSFERS_PAGE}",
            text=_protected_page_html(),
        )

        def fake_get(url: str, **_kwargs: object) -> requests.Response:
            if url == f"{BASE_URL}/":
                return _make_response(url=f"{BASE_URL}/", status_code=200)
            if url == f"{BASE_URL}{INVENTORY_TRANSFERS_PAGE}":
                return protected
            raise AssertionError(f"Unexpected GET: {url}")

        session.get.side_effect = fake_get

        with caplog.at_level("INFO"):
            login_if_needed(
                session,
                BASE_URL,
                "user",
                TEST_PASSWORD,
                verification_path=INVENTORY_TRANSFERS_PAGE,
            )

        assert INVENTORY_TRANSFERS_PAGE in caplog.text

    def test_selects_login_form_when_multiple_forms_present(self) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []

        decoy_form = (
            '<form action="/Search" method="get">'
            '<input type="text" name="query" value="">'
            "</form>"
        )
        login_html = _login_page_html(extra_forms=decoy_form)
        login_page = _make_response(url=f"{BASE_URL}/Account/LogOn", text=login_html)
        protected = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_protected_page_html(),
        )

        get_calls = {"count": 0}

        def fake_get(url: str, **_kwargs: object) -> requests.Response:
            if url == f"{BASE_URL}/":
                return _make_response(url=f"{BASE_URL}/", status_code=200)
            get_calls["count"] += 1
            if get_calls["count"] == 1:
                return login_page
            return protected

        session.get.side_effect = fake_get
        session.post.return_value = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            text=_protected_page_html(),
        )

        login_if_needed(session, BASE_URL, "user", TEST_PASSWORD)

        post_args = session.post.call_args
        posted_url = post_args[0][0]
        posted_fields = post_args[1]["data"]
        assert posted_url.endswith("/Account/LogOn")
        assert "UserName" in posted_fields
        assert "Password" in posted_fields
        assert "query" not in posted_fields


class TestSecurityDiagnostics:
    def test_exception_does_not_leak_secrets(self) -> None:
        session = MagicMock(spec=requests.Session)
        cookie = MagicMock()
        cookie.name = "ASP.NET_SessionId"
        cookie.value = COOKIE_VALUE
        session.cookies = [cookie]

        login_page = _make_response(
            url=f"{BASE_URL}/Account/LogOn",
            text=_login_page_html(),
        )

        session.get.side_effect = lambda url, **_kwargs: (
            _make_response(url=f"{BASE_URL}/", status_code=200)
            if url == f"{BASE_URL}/"
            else login_page
        )
        session.post.return_value = login_page

        with pytest.raises(AuthenticationError) as exc_info:
            login_if_needed(session, BASE_URL, "user", TEST_PASSWORD)

        error_text = str(exc_info.value)
        assert TEST_PASSWORD not in error_text
        assert COOKIE_VALUE not in error_text
        assert TEST_CSRF not in error_text

    def test_safe_response_diagnostics_strips_secrets(self) -> None:
        session = MagicMock(spec=requests.Session)
        cookie = MagicMock()
        cookie.name = "ASP.NET_SessionId"
        cookie.value = COOKIE_VALUE
        session.cookies = [cookie]

        resp = _make_response(
            url=f"{BASE_URL}/Account/LogOn?secret=abc",
            text="<html><title>Login</title></html>",
        )

        diagnostics = safe_response_diagnostics(resp, session)
        assert COOKIE_VALUE not in diagnostics
        assert "ASP.NET_SessionId" in diagnostics
        assert "secret=abc" not in diagnostics
        assert "/Account/LogOn" in diagnostics

    def test_redirect_history_strips_query_string_from_relative_location(self) -> None:
        session = MagicMock(spec=requests.Session)
        session.cookies = []

        redirect = _make_response(
            url=f"{BASE_URL}{REPORT_PAGE_PATH}",
            status_code=302,
        )
        redirect.headers["Location"] = "/Account/LogOn?ReturnUrl=%2FReports&secret=abc"
        resp = _make_response(
            url=f"{BASE_URL}/Account/LogOn?ReturnUrl=%2FReports",
            text=_login_page_html(),
            history=[redirect],
        )

        diagnostics = safe_response_diagnostics(resp, session)
        assert "ReturnUrl" not in diagnostics
        assert "secret=abc" not in diagnostics
        assert "302:/Account/LogOn" in diagnostics


class TestSalesExtractorLogin:
    @patch("pos_core.etl.raw.extraction.export_sales_report")
    @patch("pos_core.etl.raw.extraction.make_session")
    @patch("pos_core.etl.raw.extraction.login_if_needed")
    @patch("pos_core.etl.branch_config.load_branch_segments_from_json")
    def test_sales_calls_login_with_report_page_path(
        self,
        mock_load_branches: MagicMock,
        mock_login: MagicMock,
        mock_make_session: MagicMock,
        mock_export: MagicMock,
    ) -> None:
        from pathlib import Path

        from pos_core.config import DataPaths
        from pos_core.sales.extract import download_sales

        mock_load_branches.return_value = {}
        mock_make_session.return_value = MagicMock()
        mock_export.return_value = ("file.xlsx", b"data")

        paths = DataPaths.from_root(Path("/tmp/test-data"), Path("/tmp/sucursales.json"))

        with patch.dict("os.environ", {"WS_BASE": BASE_URL}):
            download_sales(paths, "2025-01-01", "2025-01-31")

        mock_login.assert_called_once()
        call_kwargs = mock_login.call_args.kwargs
        assert call_kwargs.get("verification_path") == REPORT_PAGE_PATH


class TestTransfersExtractorLogin:
    @patch("pos_core.etl.raw.extraction.export_transfers_issued")
    @patch("pos_core.etl.raw.extraction.make_session")
    @patch("pos_core.etl.raw.extraction.login_if_needed")
    def test_transfers_calls_login_with_inventory_page(
        self,
        mock_login: MagicMock,
        mock_make_session: MagicMock,
        mock_export: MagicMock,
    ) -> None:
        from pathlib import Path

        from pos_core.config import DataPaths
        from pos_core.transfers.extract import download_transfers

        mock_make_session.return_value = MagicMock()
        mock_export.return_value = ("file.xlsx", b"data")

        paths = DataPaths.from_root(Path("/tmp/test-data"), Path("/tmp/sucursales.json"))

        with (
            patch.dict("os.environ", {"WS_BASE": BASE_URL}),
            patch(
                "pos_core.transfers.extract._load_cedis_code",
                return_value="5392",
            ),
        ):
            download_transfers(paths, "2025-01-01", "2025-01-31")

        mock_login.assert_called_once()
        call_kwargs = mock_login.call_args.kwargs
        assert call_kwargs.get("verification_path") == INVENTORY_TRANSFERS_PAGE
