"""Smoke test for ETL API imports and basic functionality.

This test verifies that the ETL API can be imported and basic configuration
can be created without runtime errors.
"""

from pathlib import Path

from pos_core.etl import PaymentsETLConfig, build_payments_dataset


def test_imports_work() -> None:
    """Test that ETL API can be imported without errors."""
    assert PaymentsETLConfig is not None
    assert build_payments_dataset is not None


def test_config_creation() -> None:
    """Test that PaymentsETLConfig can be created from data_root."""
    config = PaymentsETLConfig.from_data_root(Path("data"))
    assert config is not None
    assert config.paths.raw_payments == Path("data/a_raw/payments/batch")
    assert config.paths.clean_payments == Path("data/b_clean/payments/batch")
    assert config.paths.proc_payments == Path("data/c_processed/payments")
    assert config.chunk_size_days == 180


def test_build_payments_dataset_is_callable() -> None:
    """Test that build_payments_dataset is callable."""
    assert callable(build_payments_dataset)

