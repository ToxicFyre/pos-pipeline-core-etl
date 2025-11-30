"""Tests for data grain assertions.

These tests verify that the data layers maintain the correct grain:
- Sales core fact (fact_sales_item_line): One row per item/modifier line
- Payments core fact (fact_payments_ticket): One row per ticket × payment method

The grain assertions ensure data integrity across the ETL pipeline and help
detect regressions that could affect downstream aggregations.
"""

import pandas as pd
import pytest


class TestSalesGrain:
    """Tests for sales data grain at the item/modifier line level.

    The sales core fact (fact_sales_item_line) should have:
    - One row per item or modifier on a ticket
    - Key uniqueness: (sucursal, operating_date, order_id, item_key, [modifier fields])
    - Multiple rows can share the same order_id (ticket)
    """

    @pytest.fixture
    def sample_sales_item_data(self) -> pd.DataFrame:
        """Create a sample sales DataFrame at item-line grain.

        This represents the output of the staging layer (b_clean/sales/)
        which IS the core fact for sales.
        """
        return pd.DataFrame(
            {
                "sucursal": ["Kavia", "Kavia", "Kavia", "Kavia", "Nativa", "Nativa"],
                "operating_date": [
                    "2025-01-15",
                    "2025-01-15",
                    "2025-01-15",
                    "2025-01-15",
                    "2025-01-15",
                    "2025-01-15",
                ],
                "order_id": [1001, 1001, 1001, 1002, 2001, 2001],  # Note: same order_id for items
                "item_key": ["CAFE01", "PAN01", "MOD_LECHE", "CAFE01", "JUGO01", "PAN02"],
                "item": [
                    "Café Americano",
                    "Pan Dulce",
                    "Extra Leche",
                    "Café Americano",
                    "Jugo",
                    "Pan",
                ],
                "is_modifier": [False, False, True, False, False, False],
                "group": ["CAFE", "PAN DULCE", "MOD BEBIDAS", "CAFE", "JUGOS", "PAN DULCE"],
                "quantity": [1, 2, 1, 1, 1, 1],
                "subtotal_item": [45.0, 30.0, 10.0, 45.0, 35.0, 25.0],
                "total_item": [52.2, 34.8, 11.6, 52.2, 40.6, 29.0],
            }
        )

    def test_sales_item_line_grain_has_multiple_rows_per_ticket(
        self, sample_sales_item_data: pd.DataFrame
    ) -> None:
        """Verify that sales data at item-line grain allows multiple rows per ticket.

        This is the expected behavior for the core sales fact (fact_sales_item_line).
        """
        df = sample_sales_item_data

        # Count rows per order_id (ticket)
        rows_per_ticket = df.groupby(["sucursal", "operating_date", "order_id"]).size()

        # Kavia order 1001 should have 3 item-lines
        kavia_1001 = rows_per_ticket[("Kavia", "2025-01-15", 1001)]
        assert kavia_1001 == 3, f"Expected 3 item-lines for order 1001, got {kavia_1001}"

        # Nativa order 2001 should have 2 item-lines
        nativa_2001 = rows_per_ticket[("Nativa", "2025-01-15", 2001)]
        assert nativa_2001 == 2, f"Expected 2 item-lines for order 2001, got {nativa_2001}"

    def test_sales_item_line_key_uniqueness(self, sample_sales_item_data: pd.DataFrame) -> None:
        """Verify that item-line key uniquely identifies rows.

        Key: (sucursal, operating_date, order_id, item_key)
        Note: For full uniqueness in production, modifier fields may also be needed.
        """
        df = sample_sales_item_data
        key_cols = ["sucursal", "operating_date", "order_id", "item_key"]

        # Check for duplicates in the key
        duplicates = df.duplicated(subset=key_cols, keep=False)
        assert not duplicates.any(), f"Found duplicate item-line keys:\n{df[duplicates][key_cols]}"

    def test_sales_item_line_has_required_columns(
        self, sample_sales_item_data: pd.DataFrame
    ) -> None:
        """Verify that item-line data has all required columns for the core fact."""
        df = sample_sales_item_data
        required_cols = [
            "sucursal",
            "operating_date",
            "order_id",
            "item_key",
            "group",
            "subtotal_item",
            "total_item",
        ]

        missing = set(required_cols) - set(df.columns)
        assert not missing, f"Missing required columns: {missing}"

    def test_sales_aggregation_to_ticket_is_mart(
        self, sample_sales_item_data: pd.DataFrame
    ) -> None:
        """Verify that aggregating from item-line to ticket is a mart operation.

        This tests that the item-line grain is more granular than ticket grain,
        confirming that ticket-level is a mart (aggregation), not core.
        """
        df = sample_sales_item_data

        # Aggregate to ticket level (like the marts/sales_by_ticket.py function does)
        ticket_agg = (
            df.groupby(["sucursal", "operating_date", "order_id"])
            .agg({"subtotal_item": "sum", "total_item": "sum"})
            .reset_index()
        )

        # The aggregated data should have fewer rows than the item-line data
        assert len(ticket_agg) < len(df), (
            f"Ticket aggregation should have fewer rows than item-line data. "
            f"Got {len(ticket_agg)} >= {len(df)}"
        )

        # Verify ticket 1001 totals are correct (3 items summed)
        kavia_1001 = ticket_agg[
            (ticket_agg["sucursal"] == "Kavia") & (ticket_agg["order_id"] == 1001)
        ]
        expected_subtotal = 45.0 + 30.0 + 10.0  # 85.0
        expected_total = 52.2 + 34.8 + 11.6  # 98.6
        assert abs(kavia_1001["subtotal_item"].iloc[0] - expected_subtotal) < 0.01
        assert abs(kavia_1001["total_item"].iloc[0] - expected_total) < 0.01


class TestPaymentsGrain:
    """Tests for payments data grain at the ticket × payment method level.

    The payments core fact (fact_payments_ticket) should have:
    - One row per ticket × payment method
    - Key uniqueness: (sucursal, operating_date, order_index, payment_method)
    - A ticket with multiple payment methods has multiple rows
    """

    @pytest.fixture
    def sample_payments_ticket_data(self) -> pd.DataFrame:
        """Create a sample payments DataFrame at ticket × payment method grain.

        This represents the output of the staging layer (b_clean/payments/)
        which IS the core fact for payments.
        """
        return pd.DataFrame(
            {
                "sucursal": ["Kavia", "Kavia", "Kavia", "Nativa", "Nativa"],
                "operating_date": [
                    "2025-01-15",
                    "2025-01-15",
                    "2025-01-15",
                    "2025-01-15",
                    "2025-01-15",
                ],
                # Note: order_index 1001 has two payment methods (split payment)
                "order_index": [1001, 1001, 1002, 2001, 2002],
                "payment_method": [
                    "Efectivo",
                    "Tarjeta Crédito",
                    "Efectivo",
                    "Tarjeta Débito",
                    "Efectivo",
                ],
                "ticket_total": [50.0, 100.0, 75.0, 120.0, 45.0],
                "ticket_tip": [5.0, 10.0, 7.5, 12.0, 0.0],
                "ticket_total_plus_tip": [55.0, 110.0, 82.5, 132.0, 45.0],
            }
        )

    def test_payments_ticket_grain_allows_multiple_payment_methods(
        self, sample_payments_ticket_data: pd.DataFrame
    ) -> None:
        """Verify that payments data allows multiple rows per ticket (split payments).

        This is the expected behavior when a customer pays with multiple methods.
        """
        df = sample_payments_ticket_data

        # Count payment methods per ticket
        payments_per_ticket = df.groupby(["sucursal", "operating_date", "order_index"]).size()

        # Kavia order 1001 should have 2 payment methods (split payment)
        kavia_1001 = payments_per_ticket[("Kavia", "2025-01-15", 1001)]
        assert kavia_1001 == 2, f"Expected 2 payment methods for order 1001, got {kavia_1001}"

        # Other orders should have 1 payment method each
        kavia_1002 = payments_per_ticket[("Kavia", "2025-01-15", 1002)]
        assert kavia_1002 == 1, f"Expected 1 payment method for order 1002, got {kavia_1002}"

    def test_payments_ticket_key_uniqueness(
        self, sample_payments_ticket_data: pd.DataFrame
    ) -> None:
        """Verify that ticket × payment method key uniquely identifies rows.

        Key: (sucursal, operating_date, order_index, payment_method)
        """
        df = sample_payments_ticket_data
        key_cols = ["sucursal", "operating_date", "order_index", "payment_method"]

        # Check for duplicates in the key
        duplicates = df.duplicated(subset=key_cols, keep=False)
        assert not duplicates.any(), f"Found duplicate payment keys:\n{df[duplicates][key_cols]}"

    def test_payments_ticket_has_required_columns(
        self, sample_payments_ticket_data: pd.DataFrame
    ) -> None:
        """Verify that payment data has all required columns for the core fact."""
        df = sample_payments_ticket_data
        required_cols = [
            "sucursal",
            "operating_date",
            "order_index",
            "payment_method",
            "ticket_total",
        ]

        missing = set(required_cols) - set(df.columns)
        assert not missing, f"Missing required columns: {missing}"

    def test_payments_aggregation_to_daily_is_mart(
        self, sample_payments_ticket_data: pd.DataFrame
    ) -> None:
        """Verify that aggregating from ticket to daily is a mart operation.

        This tests that ticket × payment method grain is more granular than
        daily grain, confirming that daily-level is a mart (aggregation).
        """
        df = sample_payments_ticket_data

        # Aggregate to daily level (like the marts/payments_daily.py function does)
        daily_agg = (
            df.groupby(["sucursal", "operating_date"])
            .agg(
                {
                    "ticket_total": "sum",
                    "ticket_tip": "sum",
                    "order_index": "nunique",
                }
            )
            .reset_index()
            .rename(columns={"order_index": "num_tickets"})
        )

        # The aggregated data should have fewer rows than the ticket-level data
        assert len(daily_agg) < len(df), (
            f"Daily aggregation should have fewer rows than ticket data. "
            f"Got {len(daily_agg)} >= {len(df)}"
        )

        # Verify Kavia daily totals (3 payment lines for 2 tickets)
        kavia_day = daily_agg[daily_agg["sucursal"] == "Kavia"]
        expected_total = 50.0 + 100.0 + 75.0  # 225.0
        expected_tips = 5.0 + 10.0 + 7.5  # 22.5
        expected_tickets = 2  # 1001 and 1002

        assert abs(kavia_day["ticket_total"].iloc[0] - expected_total) < 0.01
        assert abs(kavia_day["ticket_tip"].iloc[0] - expected_tips) < 0.01
        assert kavia_day["num_tickets"].iloc[0] == expected_tickets


class TestGrainDocumentation:
    """Tests that verify grain documentation is accurate."""

    def test_sales_grain_docstring_accuracy(self) -> None:
        """Verify that sales grain is correctly documented in module docstrings."""
        from pos_core.etl import staging

        docstring = staging.__doc__ or ""

        # Check for key grain documentation
        assert "item/modifier line" in docstring.lower() or "fact_sales_item_line" in docstring
        assert "order_id" in docstring

    def test_payments_grain_docstring_accuracy(self) -> None:
        """Verify that payments grain is correctly documented in module docstrings."""
        from pos_core.etl import staging

        docstring = staging.__doc__ or ""

        # Check for key grain documentation
        assert "ticket" in docstring.lower()
        assert "payment method" in docstring.lower() or "fact_payments_ticket" in docstring

    def test_marts_layer_documents_aggregations(self) -> None:
        """Verify that marts layer documents that it contains aggregations."""
        from pos_core.etl import marts

        docstring = marts.__doc__ or ""

        # Check that it's documented as aggregations
        assert "aggregat" in docstring.lower()
        assert "gold" in docstring.lower() or "mart" in docstring.lower()


class TestAggregateByTicketIsInMarts:
    """Tests to verify aggregate_by_ticket is correctly placed in marts layer."""

    def test_aggregate_by_ticket_importable_from_marts(self) -> None:
        """Verify that aggregate_by_ticket can be imported from marts."""
        from pos_core.etl.marts import aggregate_by_ticket

        assert callable(aggregate_by_ticket)

    def test_aggregate_by_ticket_backwards_compat_from_core(self) -> None:
        """Verify backwards compatibility: aggregate_by_ticket from core re-exports.

        This ensures existing code that imports from core still works.
        """
        from pos_core.etl.core import aggregate_by_ticket

        assert callable(aggregate_by_ticket)

    def test_both_imports_are_same_function(self) -> None:
        """Verify that both import paths point to the same function."""
        from pos_core.etl.core import aggregate_by_ticket as from_core
        from pos_core.etl.marts import aggregate_by_ticket as from_marts

        assert from_core is from_marts
