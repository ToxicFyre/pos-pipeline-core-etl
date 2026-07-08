"""Bronze extraction backend via pos-login-handler."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

from pos_login_handler import ExportRequest, ReportType, WansoftClient

if TYPE_CHECKING:
    from pos_core.etl.raw.extraction import ReportDescriptor

_REPORT_MAP = {
    "Payments": ReportType.PAYMENTS,
    "Detail": ReportType.DETAIL,
    "Consolidated": ReportType.CONSOLIDATED,
}

_cached_client: WansoftClient | None = None


def get_client() -> WansoftClient:
    """Return the process-wide authenticated Wansoft client."""
    global _cached_client
    if _cached_client is None:
        _cached_client = WansoftClient.shared()
    return _cached_client


def make_session(timeout: float = 60.0, retries: int = 3) -> None:
    """Return a placeholder; pos-login-handler owns the HTTP session."""
    del timeout, retries
    return None


def login_if_needed(
    s: Any,
    base_url: str,
    user: str | None,
    pwd: str | None,
    *,
    verification_path: str = "/Reports/ConsolidatedSalesMasterReport",
    **kwargs: Any,
) -> None:
    """Ensure the shared client is authenticated."""
    del s, base_url, user, pwd, verification_path, kwargs
    get_client().ensure_authenticated()


def export_sales_report(
    s: Any,
    base_url: str,
    report: str,
    subsidiary_id: str,
    start: date,
    end: date,
) -> tuple[str, bytes]:
    """Export a sales report via pos-login-handler."""
    del s, base_url

    report_key = report.capitalize()
    try:
        report_type = _REPORT_MAP[report_key]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported sales report for login_handler backend: {report!r}. "
            f"Choose from: {', '.join(_REPORT_MAP)}"
        ) from exc

    result = get_client().export(
        ExportRequest(
            report=report_type,
            subsidiary_id=str(subsidiary_id),
            start_date=start,
            end_date=end,
        )
    )
    return result.filename, result.content


def export_report(
    s: Any,
    base_url: str,
    descriptor: ReportDescriptor,
    subsidiary_id: str,
    start: date,
    end: date,
) -> tuple[str, bytes]:
    """Export a descriptor-based report via pos-login-handler."""
    del s, base_url

    if descriptor.report_name == "OrderTimes" or descriptor.export_path == "ExportOrderTimes":
        report_type = ReportType.ORDER_TIMES
    else:
        raise ValueError(f"Unsupported report descriptor for login_handler backend: {descriptor!r}")

    result = get_client().export(
        ExportRequest(
            report=report_type,
            subsidiary_id=str(subsidiary_id),
            start_date=start,
            end_date=end,
        )
    )
    return result.filename, result.content


def export_transfers_issued(
    s: Any,
    base_url: str,
    subsidiary_id: str,
    start: date,
    end: date,
) -> tuple[str, bytes]:
    """Export transfers issued report via pos-login-handler."""
    del s, base_url

    result = get_client().export(
        ExportRequest(
            report=ReportType.TRANSFERS,
            subsidiary_id=str(subsidiary_id),
            start_date=start,
            end_date=end,
        )
    )
    return result.filename, result.content
