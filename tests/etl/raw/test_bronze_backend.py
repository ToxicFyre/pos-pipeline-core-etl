"""Tests for bronze backend selection and login_handler adapter."""

from __future__ import annotations

import dataclasses
import importlib
import os
import sys
from datetime import date
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

MODULES_TO_CLEAR = (
    "pos_core.etl.raw.extraction",
    "pos_core.etl.raw.backends.login_handler",
    "pos_core.etl.raw.backends",
    "pos_login_handler",
)


class _ReportType(str, Enum):
    PAYMENTS = "payments"
    DETAIL = "detail"
    CONSOLIDATED = "consolidated"
    ORDER_TIMES = "order_times"
    TRANSFERS = "transfers"


@dataclasses.dataclass(frozen=True)
class _ExportRequest:
    report: _ReportType
    subsidiary_id: str
    start_date: date
    end_date: date


@dataclasses.dataclass(frozen=True)
class _ExportResult:
    filename: str
    content: bytes


def _install_fake_pos_login_handler() -> tuple[MagicMock, MagicMock]:
    """Inject a fake pos_login_handler module for adapter tests."""
    fake_module: Any = ModuleType("pos_login_handler")
    fake_module.ReportType = _ReportType
    fake_module.ExportRequest = _ExportRequest
    fake_module.ExportResult = _ExportResult

    mock_client = MagicMock()
    mock_shared = MagicMock(return_value=mock_client)
    fake_module.WansoftClient = MagicMock(shared=mock_shared)

    sys.modules["pos_login_handler"] = fake_module
    return mock_shared, mock_client


def _clear_modules() -> None:
    for name in MODULES_TO_CLEAR:
        sys.modules.pop(name, None)
    for name in list(sys.modules):
        if name.startswith("pos_login_handler."):
            sys.modules.pop(name, None)


def _reload_extraction(monkeypatch: pytest.MonkeyPatch, backend: str | None) -> Any:
    """Reload extraction with the requested bronze backend."""
    _clear_modules()
    if backend is None:
        monkeypatch.delenv("POS_BRONZE_BACKEND", raising=False)
    else:
        monkeypatch.setenv("POS_BRONZE_BACKEND", backend)
    _install_fake_pos_login_handler()
    return importlib.import_module("pos_core.etl.raw.extraction")


@pytest.fixture(autouse=True)
def _cleanup_bronze_backend_mocks() -> Any:
    """Remove injected mocks after each test in this module."""
    yield
    _clear_modules()
    os.environ["POS_BRONZE_BACKEND"] = "legacy_http"
    importlib.import_module("pos_core.etl.raw.extraction")


@pytest.fixture
def extraction_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Provide a reloadable extraction module for backend tests."""
    yield _reload_extraction(monkeypatch, "login_handler")
    _clear_modules()
    monkeypatch.setenv("POS_BRONZE_BACKEND", "legacy_http")
    importlib.import_module("pos_core.etl.raw.extraction")


def test_default_backend_is_login_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    extraction = _reload_extraction(monkeypatch, None)
    assert extraction._selected_bronze_backend() == "login_handler"
    assert extraction.make_session() is None


def test_legacy_http_backend_uses_requests_session(monkeypatch: pytest.MonkeyPatch) -> None:
    extraction = _reload_extraction(monkeypatch, "legacy_http")
    session = extraction.make_session()
    assert isinstance(session, requests.Session)


def test_export_sales_report_maps_payments(extraction_module: Any) -> None:
    mock_shared = sys.modules["pos_login_handler"].WansoftClient.shared
    mock_client = mock_shared.return_value
    mock_client.export.return_value = _ExportResult("payments.xlsx", b"xlsx-bytes")

    filename, content = extraction_module.export_sales_report(
        None,
        "https://example.com",
        report="Payments",
        subsidiary_id="8777",
        start=date(2025, 1, 1),
        end=date(2025, 1, 31),
    )

    assert filename == "payments.xlsx"
    assert content == b"xlsx-bytes"
    request = mock_client.export.call_args.args[0]
    assert request.report == _ReportType.PAYMENTS
    assert request.subsidiary_id == "8777"
    assert request.start_date == date(2025, 1, 1)
    assert request.end_date == date(2025, 1, 31)


def test_singleton_client_reused_for_two_exports(extraction_module: Any) -> None:
    mock_shared = sys.modules["pos_login_handler"].WansoftClient.shared
    mock_client = mock_shared.return_value
    mock_client.export.return_value = _ExportResult("fake.xlsx", b"bytes")

    extraction_module.export_sales_report(
        None,
        "https://example.com",
        report="Payments",
        subsidiary_id="8777",
        start=date(2025, 1, 1),
        end=date(2025, 1, 7),
    )
    extraction_module.export_sales_report(
        None,
        "https://example.com",
        report="Detail",
        subsidiary_id="8777",
        start=date(2025, 1, 8),
        end=date(2025, 1, 14),
    )

    assert mock_shared.call_count == 1
    assert mock_client.export.call_count == 2


def test_download_payments_reports_writes_expected_bronze_path(
    extraction_module: Any,
) -> None:
    mock_client = sys.modules["pos_login_handler"].WansoftClient.shared.return_value
    mock_client.export.return_value = _ExportResult("fake.xlsx", b"fake xlsx bytes")

    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "payments"
        sucursales_json = Path(tmpdir) / "sucursales.json"
        sucursales_json.write_text(
            '{"Kavia": {"code": "8777", "valid_from": "2024-02-21", "valid_to": null}}'
        )

        extraction_module.download_payments_reports(
            start_date="2025-01-01",
            end_date="2025-01-07",
            output_dir=output_dir,
            sucursales_json=sucursales_json,
            branches=["Kavia"],
            chunk_size_days=180,
            base_url="https://example.com",
            user="user",
            password="pass",
        )

        expected = (
            output_dir
            / "Kavia"
            / "8777"
            / "2025-01-01_2025-01-07"
            / "Payments_kavia_2025-01-01_2025-01-07.xlsx"
        )
        assert expected.exists()
        assert expected.read_bytes() == b"fake xlsx bytes"


def test_invalid_backend_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="Invalid POS_BRONZE_BACKEND"):
        _reload_extraction(monkeypatch, "unknown_backend")
