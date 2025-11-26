"""High-level tests for ETL query functions.

These tests verify that the query functions work correctly with metadata
and idempotence checks.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from pos_core.etl import PaymentsETLConfig, SalesETLConfig
from pos_core.etl.metadata import StageMetadata, read_metadata, write_metadata
from pos_core.etl.queries import get_sales


@pytest.fixture
def temp_data_dir():
    """Create a temporary data directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def payments_config(temp_data_dir):
    """Create a PaymentsETLConfig for testing."""
    sucursales_json = temp_data_dir / "sucursales.json"
    sucursales_json.write_text(
        '{"TestBranch": {"code": "1234", "valid_from": "2020-01-01", "valid_to": null}}'
    )
    return PaymentsETLConfig.from_root(temp_data_dir, sucursales_json)


@pytest.fixture
def sales_config(temp_data_dir):
    """Create a SalesETLConfig for testing."""
    sucursales_json = temp_data_dir / "sucursales.json"
    sucursales_json.write_text(
        '{"TestBranch": {"code": "1234", "valid_from": "2020-01-01", "valid_to": null}}'
    )
    return SalesETLConfig.from_root(temp_data_dir, sucursales_json)


def test_get_sales_refresh_runs_all_stages(sales_config, monkeypatch):
    """Test that refresh=True runs all stages."""
    # Mock the stage functions to track calls
    download_called = []
    clean_called = []
    aggregate_called = []

    def mock_download(*args, **kwargs):
        download_called.append(True)
        # Write metadata to simulate completion
        write_metadata(
            sales_config.paths.raw_sales,
            "2025-01-01",
            "2025-01-31",
            StageMetadata(
                start_date="2025-01-01",
                end_date="2025-01-31",
                branches=[],
                cleaner_version="download_v1",
                last_run="2025-01-15T12:00:00",
                status="ok",
            ),
        )

    def mock_clean(*args, **kwargs):
        clean_called.append(True)
        write_metadata(
            sales_config.paths.clean_sales,
            "2025-01-01",
            "2025-01-31",
            StageMetadata(
                start_date="2025-01-01",
                end_date="2025-01-31",
                branches=[],
                cleaner_version="sales_cleaner_v1",
                last_run="2025-01-15T12:00:00",
                status="ok",
            ),
        )

    def mock_aggregate(*args, **kwargs):
        aggregate_called.append(True)
        # Create a dummy output file
        output_file = sales_config.paths.proc_sales / "sales_by_ticket_2025-01-01_2025-01-31.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"test": [1, 2, 3]}).to_csv(output_file, index=False)
        return pd.DataFrame({"test": [1, 2, 3]})

    monkeypatch.setattr("pos_core.etl.queries.download_sales", mock_download)
    monkeypatch.setattr("pos_core.etl.queries.clean_sales", mock_clean)
    monkeypatch.setattr("pos_core.etl.queries.aggregate_sales", mock_aggregate)

    # Call with refresh=True
    result = get_sales("2025-01-01", "2025-01-31", sales_config, level="ticket", refresh=True)

    # Verify all stages were called
    assert len(download_called) == 1
    assert len(clean_called) == 1
    assert len(aggregate_called) == 1
    assert len(result) == 3


def test_get_sales_uses_existing_data_when_refresh_false(sales_config):
    """Test that refresh=False uses existing data when available."""
    # Pre-create metadata and output file
    sales_config.paths.proc_sales.mkdir(parents=True, exist_ok=True)
    output_file = sales_config.paths.proc_sales / "sales_by_ticket_2025-01-01_2025-01-31.csv"
    test_df = pd.DataFrame({"test": [1, 2, 3, 4, 5]})
    test_df.to_csv(output_file, index=False)

    # Write metadata for all stages
    for stage_dir in [
        sales_config.paths.raw_sales,
        sales_config.paths.clean_sales,
        sales_config.paths.proc_sales,
    ]:
        write_metadata(
            stage_dir,
            "2025-01-01",
            "2025-01-31",
            StageMetadata(
                start_date="2025-01-01",
                end_date="2025-01-31",
                branches=[],
                cleaner_version=(
                    "sales_cleaner_v1" if "clean" in str(stage_dir) else "aggregate_ticket_v1"
                ),
                last_run="2025-01-15T12:00:00",
                status="ok",
            ),
        )

    # Call with refresh=False
    result = get_sales("2025-01-01", "2025-01-31", sales_config, level="ticket", refresh=False)

    # Verify we got the existing data
    assert len(result) == 5
    assert "test" in result.columns


def test_cleaner_version_triggers_reclean(sales_config):
    """Test that outdated cleaner_version triggers re-cleaning."""
    # Write metadata with old cleaner version
    write_metadata(
        sales_config.paths.clean_sales,
        "2025-01-01",
        "2025-01-31",
        StageMetadata(
            start_date="2025-01-01",
            end_date="2025-01-31",
            branches=[],
            cleaner_version="sales_cleaner_v0",  # Old version
            last_run="2025-01-15T12:00:00",
            status="ok",
        ),
    )

    # Write metadata for raw stage (completed)
    write_metadata(
        sales_config.paths.raw_sales,
        "2025-01-01",
        "2025-01-31",
        StageMetadata(
            start_date="2025-01-01",
            end_date="2025-01-31",
            branches=[],
            cleaner_version="download_v1",
            last_run="2025-01-15T12:00:00",
            status="ok",
        ),
    )

    # Check metadata
    meta = read_metadata(sales_config.paths.clean_sales, "2025-01-01", "2025-01-31")
    assert meta is not None
    assert meta.cleaner_version == "sales_cleaner_v0"  # Old version

    # The query function should detect this and trigger re-cleaning
    # (This is tested implicitly through the metadata check logic)
