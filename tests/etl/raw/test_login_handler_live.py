"""Live tests for bronze + silver extraction via pos-login-handler."""

from __future__ import annotations

import importlib
import os
import sys
import traceback
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest

_LOGIN_HANDLER_REPO = Path(__file__).resolve().parents[3].parent / "pos-login-handler"


def _clear_login_handler_modules() -> None:
    for name in list(sys.modules):
        if name == "pos_login_handler" or name.startswith("pos_login_handler."):
            sys.modules.pop(name, None)
    for name in (
        "pos_core.etl.raw.extraction",
        "pos_core.etl.raw.backends.login_handler",
        "pos_core.etl.raw.backends",
    ):
        sys.modules.pop(name, None)


def _login_handler_available() -> bool:
    _clear_login_handler_modules()
    if _LOGIN_HANDLER_REPO.is_dir():
        repo_root = str(_LOGIN_HANDLER_REPO)
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)
    try:
        from pos_login_handler import WansoftClient
    except ImportError:
        return False
    return not isinstance(WansoftClient, MagicMock)


def _clean_env(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip('"').strip("'")


@pytest.mark.live
def test_payments_bronze_and_silver_via_login_handler() -> None:
    """Live test: payments bronze download and silver transform via login_handler."""
    if not _login_handler_available():
        pytest.skip("pos-login-handler not installed. Run: pip install -e ../pos-login-handler")

    ws_base = os.environ.get("WS_BASE")
    ws_user = os.environ.get("WS_USER")
    ws_pass = os.environ.get("WS_PASS")

    if not all([ws_base, ws_user, ws_pass]):
        pytest.skip(
            "Live test skipped: WS_BASE, WS_USER, and WS_PASS environment variables required"
        )

    os.environ["WS_BASE"] = _clean_env(ws_base)
    os.environ["WS_USER"] = _clean_env(ws_user)
    os.environ["WS_PASS"] = _clean_env(ws_pass)
    os.environ["POS_BRONZE_BACKEND"] = "login_handler"

    _clear_login_handler_modules()
    if _LOGIN_HANDLER_REPO.is_dir():
        repo_root = str(_LOGIN_HANDLER_REPO)
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)
    importlib.import_module("pos_core.etl.raw.extraction")

    from pos_core import DataPaths
    from pos_core.payments import core as payments_core
    from tests.test_utils import verify_data_retrieval

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=6)

    with TemporaryDirectory() as tmpdir:
        data_root = Path(tmpdir) / "data"
        data_root.mkdir()
        sucursales_json = data_root / "sucursales.json"
        sucursales_json.write_text(
            '{"Kavia": {"code": "8777", "valid_from": "2024-02-21", "valid_to": null}}'
        )
        paths = DataPaths.from_root(data_root, sucursales_json)

        try:
            result_df = payments_core.fetch(
                paths=paths,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                branches=["Kavia"],
                mode="force",
            )
        except Exception as exc:
            pytest.fail(
                "Live login_handler test failed with credentials provided. "
                f"Error: {exc}\n{traceback.format_exc()}"
            )

        assert result_df is not None
        assert not result_df.empty

        raw_files = list(paths.raw_payments.rglob("*.xlsx"))
        assert len(raw_files) > 0, "Expected bronze payments XLSX files"
        assert all(path.stat().st_size > 0 for path in raw_files)

        expected_columns = {"sucursal", "operating_date", "payment_method"}
        assert expected_columns.issubset(set(result_df.columns))

        verify_data_retrieval(
            paths,
            start_date.isoformat(),
            end_date.isoformat(),
            data_type="payments",
        )
