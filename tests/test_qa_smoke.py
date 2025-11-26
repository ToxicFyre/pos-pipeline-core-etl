"""Smoke test for QA API imports and basic functionality."""

import pandas as pd
from pos_core.qa import PaymentsQAResult, run_payments_qa


def test_qa_imports():
    """Test that QA API can be imported."""
    assert PaymentsQAResult is not None
    assert callable(run_payments_qa)


def test_run_payments_qa_basic():
    """Test that run_payments_qa returns a PaymentsQAResult instance."""
    # Create minimal test DataFrame
    df = pd.DataFrame(
        {
            "sucursal": ["A", "A", "B"],
            "fecha": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-01"]),
            "ingreso_efectivo": [100.0, 200.0, 150.0],
            "ingreso_credito": [50.0, 100.0, 75.0],
            "ingreso_debito": [30.0, 60.0, 45.0],
            "ingreso_amex": [0.0, 0.0, 0.0],
            "ingreso_ubereats": [0.0, 0.0, 0.0],
            "ingreso_rappi": [0.0, 0.0, 0.0],
            "ingreso_transferencia": [0.0, 0.0, 0.0],
            "ingreso_SubsidioTEC": [0.0, 0.0, 0.0],
            "ingreso_otros": [0.0, 0.0, 0.0],
            "propinas": [10.0, 20.0, 15.0],
            "num_tickets": [10, 20, 15],
        }
    )

    result = run_payments_qa(df, level=4)

    assert isinstance(result, PaymentsQAResult)
    assert isinstance(result.summary, dict)
    assert "total_rows" in result.summary
    assert "total_sucursales" in result.summary
    assert result.summary["total_rows"] == 3
    assert result.summary["total_sucursales"] == 2

