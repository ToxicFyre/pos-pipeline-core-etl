#!/usr/bin/env python3
"""Raw (Bronze) layer: POS HTTP extraction.

This module is part of the Raw (Bronze) layer in the ETL pipeline.
It handles direct data extraction from the POS system via HTTP API.

Data directory mapping:
    data/a_raw/ → Raw (Bronze) layer - Direct Wansoft exports, unchanged.

POS exporter — Sales (Detail/Consolidated) + Inventory ▸ Transfers ▸ Issued

Implements the "Aplicar" handover you recorded: runs the batch of default POSTs,
then calls the export endpoint. Works for both "Detail" and "Consolidated".

Examples:
  # Sales
  python HTTP_extraction.py --report Detail --sucursal "CEDIS" \
      --start 2025-08-01 --end 2025-08-31 --outdir ./downloads

  python HTTP_extraction.py --report Consolidated --sucursal-id 5392 \
      --start 2025-08-01 --end 2025-08-31

  # Inventory ▸ Transfers ▸ Issued
  python HTTP_extraction.py --report TransfersIssued --sucursal "CrediClub" \
      --start 2025-09-08 --end 2025-09-14 -v

Environment (optional):
  WS_BASE: Set in utils/secrets.env (see utils/secrets.env.example)
  WS_USER: Set in utils/secrets.env
  WS_PASS: Set in utils/secrets.env
  WS_SUCURSALES=/path/to/sucursales.json   # {"CEDIS": "5392", "CrediClub": "10075", ...}
  WS_TIMEOUT=60   # seconds
  WS_RETRIES=3

Notes:
- If login is required in your tenant, populate WS_USER/WS_PASS.
- CSRF token is auto-detected.
- For Sales, the "Aplicar" sequence is posted before the export to match the browser.

"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import json
import logging
import os
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag  # pip install requests bs4
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pos_core.etl.branch_config import load_branch_segments_from_json
from pos_core.etl.utils import (
    discover_existing_intervals,
    iter_chunks,
    parse_date,
    slugify,
    subtract_intervals,
)

# ------------------------- Config -------------------------
DEFAULT_BASE = os.environ.get("WS_BASE")

REPORT_ENDPOINTS = {
    "Detail": "ExportSalesDetailReport",
    "Consolidated": "Export",
    "Payments": "ExportSalesReport",
}

REPORT_PAGE_PATH = "/Reports/ConsolidatedSalesMasterReport"

INVENTORY_TRANSFERS_PAGE = "/Inventory/Transfers"
INVENTORY_TRANSFERS_EXPORT = "/Inventory/ExportTransfersIssued"

# Fallback in-code map; override via WS_SUCURSALES JSON if present.
SUCURSAL_DICT_FALLBACK = {
    "CrediClub": "10075",
    "CEDIS": "5392",
}

# "Aplicar" endpoints captured from your test notebook
APLICAR_ENDPOINTS = [
    "GetConsolidatedSales",
    "CancelSalesDetail",
    "CourtesiesDetail",
    "SalesByHours",
    "SalesByGroup",
    "SalesByGroupType",
    "SalesByArea",
    "SalesBySaucer",
    "SalesByUser",
    "SalesByTypeOfOrder",
    "DiscountsDetail",
    "PersonsByHour",
    "PersonsByDay",
    "PersonsByDayName",
    "SalesByPaymentType",
    "SalesByModifiers",
    "SalesByTerminal",
    "MegaPointsReport",
    "TipByUser",
    "Promotions",
    "ChargePaymentMethod",
    "SaleNullificationDetail",
]


# ------------------------- Helpers -------------------------
def load_sucursal_map() -> dict[str, str]:
    """Load sucursales mapping from WS_SUCURSALES.

    Supports two JSON shapes:

      {
        "Kavia": "8777",
        "QIN": "6190"
      }

    and the richer form with validity dates:

      {
        "Kavia": {
          "code": "8777",
          "valid_from": "2024-02-21",
          "valid_to": null
        },
        "Kavia_OLD": {
          "code": "6161",
          "valid_from": "2022-11-01",
          "valid_to": "2024-02-20"
        }
      }

    In all cases this function returns a simple dict:
      { "Kavia": "8777", "Kavia_OLD": "6161", ... }
    """
    path = os.environ.get("WS_SUCURSALES")
    if path and Path(path).exists():
        try:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("WS_SUCURSALES must be a JSON object mapping name -> value")

            mapping: dict[str, str] = {}
            for k, v in raw.items():
                name = str(k)
                if isinstance(v, str):
                    # old style: "Kavia": "8777"
                    mapping[name] = str(v)
                elif isinstance(v, dict):
                    # new style: "Kavia": {"code": "8777", "valid_from": "...", "valid_to": null}
                    if "code" not in v:
                        raise ValueError(f"Entry {name!r} is an object but has no 'code' field")
                    mapping[name] = str(v["code"])
                else:
                    raise ValueError(
                        f"Entry {name!r} has unsupported type {type(v).__name__} "
                        f"(expected string or object with 'code')"
                    )

            return mapping

        except (ValueError, json.JSONDecodeError, OSError) as e:
            logging.warning("Failed to load WS_SUCURSALES (%s): %s", path, e)

    # Fallback to in-code mapping if file/env is missing or invalid
    return SUCURSAL_DICT_FALLBACK.copy()


def ensure_ok(resp: requests.Response, msg: str) -> None:
    """Check if HTTP response is successful, raise SystemExit if not.

    Args:
        resp: HTTP response object to check.
        msg: Error message prefix if response is not successful.

    Raises:
        SystemExit: If response status code is not in 200-299 range.

    """
    if not (200 <= resp.status_code < 300):
        raise SystemExit(f"{msg}. HTTP {resp.status_code} — {resp.text[:400]}")


def _attr_to_str(attr: Any) -> str:
    """Convert BeautifulSoup attribute value to string."""
    if attr is None:
        return ""
    if isinstance(attr, list):
        return str(attr[0]) if attr else ""
    return str(attr)


def get_csrf_from_html(html: str) -> str | None:
    """Extract CSRF token from HTML page.

    Searches for ASP.NET AntiForgery tokens in common locations:
    - Input fields named "__RequestVerificationToken" or similar
    - Meta tags with "__RequestVerificationToken" name
    - Hidden input fields containing "VerificationToken" in name/id

    Args:
        html: HTML content to parse.

    Returns:
        CSRF token string if found, None otherwise.

    """
    soup = BeautifulSoup(html, "html.parser")
    # Common names used by ASP.NET AntiForgery
    for name in ["__RequestVerificationToken", "__RequestVerificationTokenWith"]:
        tag = soup.find("input", attrs={"name": name})
        if isinstance(tag, Tag):
            value = _attr_to_str(tag.get("value"))
            if value:
                return value
    m = soup.find("meta", attrs={"name": "__RequestVerificationToken"})
    if isinstance(m, Tag):
        content = _attr_to_str(m.get("content"))
        if content:
            return content
    # Fallback: any hidden with VerificationToken in the name
    for tag in soup.find_all("input", attrs={"type": "hidden"}):
        if isinstance(tag, Tag):
            name_attr = _attr_to_str(tag.get("name"))
            id_attr = _attr_to_str(tag.get("id"))
            nm = name_attr + id_attr
            if "VerificationToken" in nm:
                value = _attr_to_str(tag.get("value"))
                if value:
                    return value
    return None


def require_csrf_token(
    token: str | None,
    *,
    context: str,
    response: requests.Response,
    session: requests.Session,
) -> str:
    """Ensure a CSRF token is present; otherwise fail with useful diagnostics.

    This function is MANDATORY for all POS interactions. The pipeline will
    crash immediately if a CSRF token cannot be found, preventing 401 errors
    later in the workflow.

    Args:
        token: Parsed token (may be None).
        context: Human-friendly description of where we looked for the token.
        response: HTTP response object containing the HTML.
        session: Current session (used to introspect cookies).

    Returns:
        The non-empty CSRF token (guaranteed to be non-None and non-empty).

    Raises:
        SystemExit: If token is missing or empty, with detailed diagnostics
            to help debug the root cause. The pipeline will NOT proceed without
            a valid token.

    """
    if token and token.strip():
        return token

    try:
        soup = BeautifulSoup(response.text, "html.parser")
        title = (soup.title.string or "").strip() if soup.title and soup.title.string else "n/a"
    except (AttributeError, TypeError):
        title = "n/a"
    auth_cookie = any(c.name.upper().startswith(".ASPXAUTH") for c in session.cookies)
    login_redirected = bool(response.url and "/Account/LogOn" in response.url)
    snippet = (response.text or "").replace("\r", " ").replace("\n", " ")[:240]

    # Check if we searched for common token field names
    token_fields_searched = ["__RequestVerificationToken", "__RequestVerificationTokenWith"]
    token_found_in_html = False
    try:
        for field_name in token_fields_searched:
            if field_name in (response.text or ""):
                token_found_in_html = True
                break
    except (AttributeError, TypeError):
        pass

    raise SystemExit(
        f"CSRF token is MANDATORY but was not found on {context}. "
        f"The pipeline will crash here to prevent 401 errors later. "
        f"Diagnostics: status={response.status_code}, url={response.url}, "
        f"login_redirected={login_redirected}, auth_cookie_present={auth_cookie}, "
        f"content_type={response.headers.get('Content-Type')}, "
        f"token_fields_searched={token_fields_searched}, "
        f"token_field_found_in_html={token_found_in_html}, "
        f"HTML title={title}. Body start: {snippet}"
    )


# --- HTTP resiliency ---
DEFAULT_TIMEOUT = float(os.environ.get("WS_TIMEOUT", "60"))
DEFAULT_RETRIES = int(os.environ.get("WS_RETRIES", "3"))


def make_session(
    timeout: float = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES
) -> requests.Session:
    """Create a requests Session with retry logic and default timeout.

    Configures the session with:
    - User-Agent header (Mozilla/5.0)
    - Retry adapter for HTTP/HTTPS with exponential backoff
    - Default timeout for all requests
    - Retries on 429, 500, 502, 503, 504 status codes

    Args:
        timeout: Default timeout in seconds for all requests. Defaults to
            DEFAULT_TIMEOUT (60 seconds).
        retries: Number of retry attempts. Defaults to DEFAULT_RETRIES (3).

    Returns:
        Configured requests.Session object.

    """
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=0.8,  # 0.8, 1.6, 3.2, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST", "HEAD", "OPTIONS"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    # Default timeouts via a wrapper
    orig_request = s.request

    def timed_request(method: str, url: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", timeout)
        return orig_request(method, url, **kwargs)

    s.request = timed_request  # type: ignore[method-assign,assignment]
    return s


def choose_user_field(fields: dict[str, str]) -> str | None:
    """Identify the username field name from form fields.

    Searches for common username field names in order of preference.

    Args:
        fields: Dictionary of form field names to values.

    Returns:
        Field name if found, None otherwise.

    """
    for cand in ("UserName", "Email", "Login", "Username"):
        if cand in fields:
            return cand
    return None


def choose_password_field(fields: dict[str, str], html: str) -> str | None:
    """Identify the password field name from form fields or HTML.

    First searches form fields dictionary, then parses HTML for
    password input elements.

    Args:
        fields: Dictionary of form field names to values.
        html: HTML content to parse if field not found in dictionary.

    Returns:
        Field name if found, None otherwise.

    """
    for cand in ("Password", "Pass", "Pwd"):
        if cand in fields:
            return cand
    # Try from input type="password"
    soup = BeautifulSoup(html, "html.parser")
    pwd = soup.find("input", attrs={"type": "password"})
    if isinstance(pwd, Tag):
        name_attr = _attr_to_str(pwd.get("name"))
        if name_attr:
            return name_attr
    return None


def _origin_for(base_url: str) -> str:
    """Extract origin (scheme + netloc) from a URL.

    Args:
        base_url: Full URL string.

    Returns:
        Origin string (e.g., from WS_BASE environment variable).

    """
    p = urlparse(base_url)
    return f"{p.scheme}://{p.netloc}"


def login_if_needed(s: requests.Session, base_url: str, user: str | None, pwd: str | None) -> None:
    """Authenticate with POS if login is required.

    Attempts to access a protected page. If redirected to login, automatically
    parses the login form, fills credentials, and submits. Verifies successful
    authentication by checking access to the report page.

    Args:
        s: Requests session object.
        base_url: Base URL of POS instance.
        user: Username for authentication (from WS_USER env var if None).
        pwd: Password for authentication (from WS_PASS env var if None).

    Raises:
        SystemExit: If login is required but credentials are missing, or if
            login fails after submission.

    """
    # Get credentials from environment if not provided
    if user is None:
        user = os.environ.get("WS_USER")
    if pwd is None:
        pwd = os.environ.get("WS_PASS")

    # Seed the session on tenant root (sets cookies that some auth flows expect)
    seed = s.get(f"{base_url}/")
    if seed.status_code not in (200, 302):
        logging.debug("Seed GET returned %s", seed.status_code)

    # Try hitting a protected page to trigger login redirect
    r = s.get(f"{base_url}{REPORT_PAGE_PATH}", allow_redirects=True)
    if r.url.endswith("/Account/LogOn") or "/Account/LogOn" in r.url or r.status_code in (401,):
        if not user or not pwd:
            raise SystemExit("Login required but WS_USER/WS_PASS not provided.")

        # Parse login form
        page_url = r.url
        soup = BeautifulSoup(r.text, "html.parser")
        form = soup.find("form")
        if not isinstance(form, Tag):
            raise SystemExit("Login form not found.")
        action_attr = form.get("action")
        action = _attr_to_str(action_attr) if action_attr else page_url
        action_url = action if action.startswith("http") else f"{_origin_for(base_url)}{action}"

        fields: dict[str, str] = {}
        for inp in form.find_all("input"):
            if isinstance(inp, Tag):
                name = _attr_to_str(inp.get("name"))
                if not name:
                    continue
                value = _attr_to_str(inp.get("value"))
                fields[name] = value

        user_field = choose_user_field(fields) or "UserName"
        pw_field = choose_password_field(fields, r.text) or "Password"
        if user_field not in fields or pw_field not in fields:
            raise SystemExit(
                f"Could not identify user/password fields. Found: {list(fields.keys())}"
            )

        fields[user_field] = user
        fields[pw_field] = pwd
        if "ReturnUrl" in fields and not fields["ReturnUrl"]:
            fields["ReturnUrl"] = REPORT_PAGE_PATH

        headers = {"Referer": page_url, "Origin": _origin_for(base_url)}
        r2 = s.post(action_url, data=fields, headers=headers, allow_redirects=True, timeout=100)
        if r2.status_code not in (200, 302):
            raise SystemExit(f"Login POST failed. HTTP {r2.status_code}")
        # quick auth check
        test = s.get(f"{base_url}{REPORT_PAGE_PATH}")
        if test.status_code in (200,):
            logging.info("Login succeeded")
            return
        aspxauth = [c for c in s.cookies if c.name.upper().startswith(".ASPXAUTH")]
        raise SystemExit(
            "Login failed: still redirected to login. "
            f"Auth cookie present: {bool(aspxauth)}; final URL checked: {test.url}"
        )
    else:
        logging.info("No login required.")


# ------------------------- Warm-up helpers -------------------------
def _set_subsidiary_cookie(s: requests.Session, base_url: str, subsidiary_id: str) -> None:
    """Set SubsidiaryId cookie in the session.

    This cookie is required by POS to identify which branch/subsidiary
    to query. Sets the cookie for the domain extracted from base_url.

    Args:
        s: Requests session object.
        base_url: Base URL to extract domain from.
        subsidiary_id: Subsidiary/branch ID to set in cookie.

    """
    try:
        dom = urlparse(base_url).hostname
        if dom:
            s.cookies.set("SubsidiaryId", str(subsidiary_id), domain=dom, path="/")
            logging.debug("Set cookie SubsidiaryId=%s for %s", subsidiary_id, dom)
    except (AttributeError, ValueError, TypeError):
        pass


def aplicar_warmup(
    s: requests.Session,
    base_url: str,
    report_page_url: str,
    token: str,
    subsidiary_id: str,
    start: date,
    end: date,
) -> None:
    """Execute the "Aplicar" warm-up sequence before exporting reports.

    POS requires a series of AJAX POST requests to various endpoints
    before the export will work. This function mirrors the browser behavior
    by posting to all required endpoints with the date range parameters.

    This is necessary because POS's export endpoint expects the report
    data to be pre-loaded via these warm-up calls.

    Args:
        s: Authenticated requests session.
        base_url: Base URL of POS instance.
        report_page_url: URL of the report page (for Referer header).
        token: CSRF token (required).
        subsidiary_id: Branch/subsidiary ID.
        start: Start date for the report.
        end: End date for the report.

    Raises:
        SystemExit: If token is missing/empty, authentication fails (401),
            or CSRF/policy blocks (400/403).

    """
    if not token or not token.strip():
        raise SystemExit(
            "CSRF token is required for aplicar_warmup() but was None or empty. "
            "The pipeline cannot proceed without a valid CSRF token. "
            "Ensure require_csrf_token() is called before this function."
        )

    ajax_headers = {
        "Origin": _origin_for(base_url),
        "Referer": report_page_url,
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "*/*",
        "RequestVerificationToken": token,
    }
    params = {
        "subsidiaryId": str(subsidiary_id),
        "startDate": start.strftime("%Y-%m-%d"),
        "endDate": end.strftime("%Y-%m-%d"),
    }
    body = dict(params)
    body["__RequestVerificationToken"] = token

    # quick self-test then full batch; errors are raised for 400/401/403, others logged
    def post_endpoint(name: str) -> None:
        url = f"{base_url}/Reports/{name}"
        r = s.post(url, params=params, data=body, headers=ajax_headers)
        if r.status_code == 401:
            raise SystemExit("401 Not authenticated (lost .ASPXAUTH?)")
        if r.status_code in (400, 403):
            raise SystemExit(f"{r.status_code} CSRF/Policy block on {name}")
        if not (200 <= r.status_code < 300):
            logging.warning("Warm-up %s returned %s", name, r.status_code)

    # One probe first then the batch
    post_endpoint(APLICAR_ENDPOINTS[0])
    for ep in APLICAR_ENDPOINTS:
        post_endpoint(ep)


# ------------------------- Exporters -------------------------
def export_sales_report(
    s: requests.Session,
    base_url: str,
    report: str,
    subsidiary_id: str,
    start: date,
    end: date,
) -> tuple[str, bytes]:
    """Export a sales report from POS API.

    Handles the complete export workflow:
    1. Sets SubsidiaryId cookie
    2. Retrieves CSRF token from report page
    3. Executes "Aplicar" warm-up sequence
    4. Calls export endpoint
    5. Parses response (JSON with base64 file or direct file download)

    Args:
        s: Authenticated requests session.
        base_url: Base URL of POS instance.
        report: Report type ("Detail", "Consolidated", or "Payments").
        subsidiary_id: Branch/subsidiary ID.
        start: Start date for the report (inclusive).
        end: End date for the report (inclusive).

    Returns:
        Tuple of (suggested_filename, xlsx_bytes).

    Raises:
        SystemExit: If report type is unknown, export fails, or response
            format is unexpected.

    """
    report = report.capitalize()
    endpoint = REPORT_ENDPOINTS.get(report)
    if not endpoint:
        raise SystemExit(
            f"Unknown sales report '{report}'. Choose from: {', '.join(REPORT_ENDPOINTS)}"
        )

    # 0) Set SubsidiaryId cookie up-front (matches your note + notebook)
    _set_subsidiary_cookie(s, base_url, subsidiary_id)

    # 1) GET report page to obtain CSRF token
    report_page = f"{base_url}{REPORT_PAGE_PATH}"
    r = s.get(report_page)
    ensure_ok(r, "Failed to open report page")
    token = require_csrf_token(
        get_csrf_from_html(r.text),
        context="Sales report page (Reports/ConsolidatedSalesMasterReport)",
        response=r,
        session=s,
    )
    # require_csrf_token() guarantees non-empty token (raises SystemExit if missing)
    assert token and token.strip(), "Token must be non-empty after require_csrf_token()"

    # 2) Run the "Aplicar" warm-up set
    aplicar_warmup(s, base_url, report_page, token, subsidiary_id, start, end)

    # 3) Export call: params in QS, same params in body, token in body, browser-like headers
    export_url = f"{base_url}/Reports/{endpoint}"
    params = {
        "subsidiaryId": str(subsidiary_id),
        "startDate": start.strftime("%Y-%m-%d"),
        "endDate": end.strftime("%Y-%m-%d"),
    }
    body = dict(params)
    headers = {
        "Origin": _origin_for(base_url),
        "Referer": report_page,
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "*/*",
        "RequestVerificationToken": token,
    }
    body["__RequestVerificationToken"] = token

    r = s.post(
        export_url, params=params, data=body, headers=headers, allow_redirects=True, timeout=120
    )
    if r.status_code == 401:
        raise SystemExit("401 Unauthorized on export — auth expired or CSRF missing.")
    ensure_ok(r, f"Export failed for {report} {subsidiary_id} {start}..{end}")

    # 4) Accept JSON {fileBase64} or a direct file response
    ct = (r.headers.get("Content-Type") or "").lower()
    if "application/json" in ct:
        j = r.json()
        if "fileBase64" not in j:
            raise SystemExit(f"Export JSON missing 'fileBase64'. Keys: {list(j.keys())}")
        fname = j.get("fileName") or f"{report}.xlsx"
        content = base64.b64decode(j["fileBase64"])
        return fname, content

    cd = r.headers.get("Content-Disposition") or ""
    if "application/vnd" in ct or "application/octet-stream" in ct or "attachment" in cd.lower():
        fname = _content_disposition_filename(cd) or f"{report}_{start}_{end}.xlsx"
        return fname, r.content

    # If it came back HTML, show the title/first bytes to help debug
    raise SystemExit(
        f"Export returned unexpected content-type {ct}. Body starts: {(r.text or '')[:300]}"
    )


def _content_disposition_filename(h: str | None) -> str | None:
    """Extract filename from Content-Disposition header.

    Args:
        h: Content-Disposition header value.

    Returns:
        Filename if found, None otherwise.

    """
    if not h:
        return None
    m = re.search(r'filename\\*?=(?:UTF-8\\\'\\\')?"?([^";]+)"?', h)
    return m.group(1) if m else None


def export_transfers_issued(
    s: requests.Session,
    base_url: str,
    subsidiary_id: str,
    start: date,
    end: date,
) -> tuple[str, bytes]:
    """Export Inventory ▸ Transfers ▸ Issued report from POS.

    Exports transfer data for issued transfers within the date range.
    Uses form-urlencoded POST with CSRF token and SubsidiaryId cookie.

    Args:
        s: Authenticated requests session.
        base_url: Base URL of POS instance.
        subsidiary_id: Branch/subsidiary ID.
        start: Start date for the report (inclusive).
        end: End date for the report (inclusive).

    Returns:
        Tuple of (suggested_filename, xlsx_bytes).

    Raises:
        SystemExit: If export fails, authentication is lost, or response
            format is unexpected.

    """
    # 1) Open page to get CSRF + set cookie
    page_url = f"{base_url}{INVENTORY_TRANSFERS_PAGE}"
    r = s.get(page_url)
    ensure_ok(r, "Failed to open Inventory ▸ Transfers page")
    token = require_csrf_token(
        get_csrf_from_html(r.text),
        context="Inventory ▸ Transfers page",
        response=r,
        session=s,
    )
    # require_csrf_token() guarantees non-empty token (raises SystemExit if missing)
    assert token and token.strip(), "Token must be non-empty after require_csrf_token()"

    _set_subsidiary_cookie(s, base_url, subsidiary_id)

    # 2) POST export
    url = f"{base_url}{INVENTORY_TRANSFERS_EXPORT}"
    form = {
        "subsidiaryId": str(subsidiary_id),
        "startDate": start.strftime("%Y-%m-%d"),
        "endDate": end.strftime("%Y-%m-%d"),
        "transferReference": "",
        "status": "0",
    }
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "*/*",
        "Referer": page_url,
        "Origin": _origin_for(base_url),
        "RequestVerificationToken": token,
    }
    form["__RequestVerificationToken"] = token

    r = s.post(url, data=form, headers=headers, allow_redirects=True, timeout=120)
    if r.status_code == 401:
        aspxauth = [c for c in s.cookies if c.name.upper().startswith(".ASPXAUTH")]
        raise SystemExit(
            "ExportTransfersIssued returned 401 (unauthorized). "
            f"Auth cookie present: {bool(aspxauth)}. "
            "Likely the login didn't stick or the CSRF token is missing."
        )
    ensure_ok(r, "ExportTransfersIssued failed")

    # Response is usually JSON with fileBase64, but accept attachment too
    ct = (r.headers.get("Content-Type") or "").lower()
    if "application/json" in ct:
        j = r.json()
        if "fileBase64" not in j:
            raise SystemExit(f"Inventory export JSON missing 'fileBase64'. Keys: {list(j.keys())}")
        fname = j.get("fileName") or "TransfersIssued.xlsx"
        return fname, base64.b64decode(j["fileBase64"])

    cd = r.headers.get("Content-Disposition") or ""
    if "application/vnd" in ct or "application/octet-stream" in ct or "attachment" in cd.lower():
        fname = _content_disposition_filename(cd) or f"TransfersIssued_{start}_{end}.xlsx"
        return fname, r.content

    raise SystemExit(
        f"Inventory export returned unexpected content-type {ct}. "
        f"Body starts: {(r.text or '')[:300]}"
    )


# ------------------------- Payments ETL Function -------------------------

logger = logging.getLogger(__name__)


def download_payments_reports(
    start_date: str,
    end_date: str,
    output_dir: Path | str,
    sucursales_json: Path | str,
    branches: list[str] | None = None,
    chunk_size_days: int = 180,
    base_url: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> None:
    """Download payments reports for all branches within a date range.

    Downloads missing payment reports from the POS HTTP API, respecting branch
    code windows and skipping already-downloaded date ranges. Handles chunking
    large date ranges into smaller HTTP requests.

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        output_dir: Directory to save downloaded Excel files. Will be created if it doesn't exist.
        sucursales_json: Path to sucursales.json configuration file containing branch definitions.
        branches: Optional list of branch names to process. If None, processes all branches.
        chunk_size_days: Maximum number of days per HTTP request chunk (default: 180).
        base_url: Optional base URL for POS API. If None, uses DEFAULT_BASE from environment.
        user: Optional username for authentication. If None, uses WS_USER from environment.
        password: Optional password for authentication. If None, uses WS_PASS from environment.

    Raises:
        FileNotFoundError: If sucursales_json doesn't exist.
        ValueError: If start_date or end_date is invalid.

    Examples:
        >>> from pathlib import Path
        >>> download_payments_reports(
        ...     "2023-01-01",
        ...     "2023-12-31",
        ...     Path("data/a_raw/payments/batch"),
        ...     Path("utils/sucursales.json"),
        ...     chunk_size_days=90
        ... )

    """
    # Convert string paths to Path
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    if isinstance(sucursales_json, str):
        sucursales_json = Path(sucursales_json)

    if not sucursales_json.exists():
        raise FileNotFoundError(f"Sucursales JSON file not found: {sucursales_json}")

    # Parse dates
    try:
        global_start = parse_date(start_date)
        global_end = parse_date(end_date)
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD: {e}") from e

    if global_start > global_end:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")

    # Use base_url from parameter or fall back to DEFAULT_BASE
    if base_url is None:
        base_url = DEFAULT_BASE
        if base_url is None:
            raise ValueError(
                "base_url must be provided or WS_BASE environment variable must be set"
            )
    base_url = base_url.rstrip("/")

    # Get user/password from parameters or environment
    if user is None:
        user = os.environ.get("WS_USER")
    if password is None:
        password = os.environ.get("WS_PASS")

    # Load branch segments
    branch_segments = load_branch_segments_from_json(sucursales_json)

    # Filter branches if specified
    if branches is not None:
        branch_segments = {
            name: windows for name, windows in branch_segments.items() if name in branches
        }
        if not branch_segments:
            logger.warning(f"No matching branches found in filter: {branches}")

    # Discover existing intervals
    existing_by_code = discover_existing_intervals(output_dir)
    logger.info(f"Found existing intervals for {len(existing_by_code)} branch code(s)")

    # Create session and authenticate
    s = make_session()
    login_if_needed(s, base_url, user, password)

    # Download missing chunks
    from datetime import timedelta

    for branch_name, windows in branch_segments.items():
        logger.info(f"Processing branch: {branch_name}")
        for seg in windows:
            # Calculate intersection of code window with requested date range
            seg_start = max(global_start, seg.valid_from)
            seg_end = min(global_end, seg.valid_to or global_end)
            if seg_start > seg_end:
                continue

            code = seg.code
            # Get already-downloaded intervals for this code
            already = existing_by_code.get(code, [])
            # Find gaps: date ranges that need to be downloaded
            missing_ranges = subtract_intervals((seg_start, seg_end), already)

            if not missing_ranges:
                logger.debug(
                    f"  code={code} window {seg_start}..{seg_end}: already fully covered, skipping."
                )
                continue

            code_root = output_dir / branch_name / code
            code_root.mkdir(parents=True, exist_ok=True)

            logger.info(f"  code={code} window {seg_start}..{seg_end}")
            logger.debug(f"    existing: {already or 'none'}")
            logger.debug(f"    missing ranges: {missing_ranges}")

            for mr_start, mr_end in missing_ranges:
                chunks = iter_chunks(mr_start, mr_end, chunk_size_days)
                for chunk_start, chunk_end in chunks:
                    chunk_dir = code_root / f"{chunk_start}_{chunk_end}"
                    chunk_dir.mkdir(parents=True, exist_ok=True)

                    logger.info(f"    downloading {chunk_start}..{chunk_end} -> {chunk_dir}")
                    # POS API treats end date as exclusive, so we add 1 day
                    # to ensure we get data for the full chunk_end date
                    api_end_date = chunk_end + timedelta(days=1)

                    # Export the report
                    suggested, blob = export_sales_report(
                        s=s,
                        base_url=base_url,
                        report="Payments",
                        subsidiary_id=code,
                        start=chunk_start,
                        end=api_end_date,
                    )

                    # Save file
                    out_name = build_out_name(
                        "Payments", branch_name, chunk_start, chunk_end, suggested
                    )
                    out_path = chunk_dir / out_name
                    out_path.write_bytes(blob)
                    logger.debug(f"Saved {out_path} ({len(blob)} bytes)")


# ------------------------- CLI -------------------------
@dataclasses.dataclass
class Args:
    report: str
    base: str
    sucursal: str | None
    sucursal_id: str | None
    start: date
    end: date
    outdir: Path
    user: str | None
    password: str | None
    verbose: bool


def build_out_name(kind: str, sucursal_name: str, start: date, end: date, _suggested: str) -> str:
    """Build output filename for downloaded report.

    Creates a standardized filename: {kind}_{sucursal_slug}_{start}_{end}.xlsx

    Args:
        kind: Report type (e.g., "Detail", "Payments").
        sucursal_name: Branch name (will be slugified).
        start: Start date.
        end: End date.
        _suggested: Suggested filename from API (unused, kept for compatibility).

    Returns:
        Filename string with .xlsx extension.

    """
    base = f"{kind}_{slugify(sucursal_name)}_{start.isoformat()}_{end.isoformat()}"
    return base + ".xlsx"


def choose_sucursal_id(
    suc_map: dict[str, str], name: str | None, explicit_id: str | None
) -> tuple[str, str]:
    """Resolve sucursal ID and friendly name from arguments.

    Determines the numeric ID and friendly name to use based on provided
    arguments. Explicit ID takes precedence, then name lookup, then fallback.

    Args:
        suc_map: Dictionary mapping branch names to IDs.
        name: Branch name (optional).
        explicit_id: Explicit numeric ID (optional, takes precedence).

    Returns:
        Tuple of (sucursal_id, friendly_name).

    Raises:
        SystemExit: If name is provided but not found in suc_map and is not numeric.

    """
    if explicit_id:
        # Find a friendly name for logs/filename if possible
        for k, v in suc_map.items():
            if str(v) == str(explicit_id):
                return str(v), k
        return str(explicit_id), str(explicit_id)
    if name:
        name = str(name)
        if name in suc_map:
            return suc_map[name], name
        # Allow passing the name actually being an ID
        if name.isdigit():
            return name, name
        raise SystemExit(f"Sucursal '{name}' not found. Known: {', '.join(sorted(suc_map))}")
    # Fallback to first known
    k, v = next(iter(suc_map.items()))
    return v, k


def parse_args() -> Args:
    p = argparse.ArgumentParser(description="POS report exporter")
    p.add_argument(
        "--report", required=True, choices=["Detail", "Consolidated", "TransfersIssued", "Payments"]
    )
    p.add_argument("--base", default=DEFAULT_BASE)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--sucursal", help="Sucursal name (e.g., 'CEDIS')")
    g.add_argument("--sucursal-id", help="Sucursal numeric ID")
    p.add_argument("--start", required=True, type=parse_date)
    p.add_argument("--end", required=True, type=parse_date)
    p.add_argument("--outdir", default=Path("./downloads"), type=Path)
    p.add_argument("--user", default=os.environ.get("WS_USER"))
    p.add_argument("--password", default=os.environ.get("WS_PASS"))
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    return Args(
        report=args.report,
        base=args.base.rstrip("/"),
        sucursal=args.sucursal,
        sucursal_id=args.sucursal_id,
        start=args.start,
        end=args.end,
        outdir=args.outdir,
        user=args.user,
        password=args.password,
        verbose=args.verbose,
    )


def main() -> None:
    """Execute the HTTP extraction command-line tool.

    Orchestrates the complete export workflow:
    1. Parses command-line arguments
    2. Loads sucursal mapping
    3. Authenticates if needed
    4. Exports the requested report
    5. Saves file to output directory

    Raises:
        SystemExit: On various error conditions (authentication, export failure, etc.).

    """
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    suc_map = load_sucursal_map()
    suc_id, suc_name = choose_sucursal_id(suc_map, args.sucursal, args.sucursal_id)

    s = make_session()
    login_if_needed(s, args.base, args.user, args.password)

    if args.report in ("Detail", "Consolidated", "Payments"):
        suggested, blob = export_sales_report(
            s=s,
            base_url=args.base,
            report=args.report,
            subsidiary_id=suc_id,
            start=args.start,
            end=args.end,
        )
    elif args.report == "TransfersIssued":
        suggested, blob = export_transfers_issued(
            s=s,
            base_url=args.base,
            subsidiary_id=suc_id,
            start=args.start,
            end=args.end,
        )
    else:
        raise SystemExit(f"Unknown report kind {args.report}")

    args.outdir.mkdir(parents=True, exist_ok=True)
    out_name = build_out_name(args.report, suc_name, args.start, args.end, suggested)
    out_path = args.outdir / out_name
    out_path.write_bytes(blob)
    logging.info("Saved %s (%d bytes)", out_path, len(blob))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
