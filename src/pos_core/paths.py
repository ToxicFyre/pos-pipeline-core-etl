"""Data paths configuration for POS Core ETL.

This module provides the DataPaths class for managing filesystem paths
across all ETL layers (bronze/silver/gold).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class DataPaths:
    """All filesystem paths used by the ETL pipelines.

    This unified configuration replaces the domain-specific configs
    (PaymentsETLConfig, SalesETLConfig) with a single, simpler interface.

    Attributes:
        data_root: Root directory for all ETL data layers.
        sucursales_json: Path to branch configuration JSON file.

    Directory Structure:
        data_root/
        ├── a_raw/           # Bronze: raw Wansoft exports
        │   ├── payments/
        │   ├── sales/
        │   └── order_times/
        ├── b_clean/         # Silver: core facts at atomic grain
        │   ├── payments/    # fact_payments_ticket
        │   ├── sales/       # fact_sales_item_line
        │   └── order_times/
        └── c_processed/     # Gold: marts (aggregated tables)
            ├── payments/    # mart_payments_daily
            ├── sales/       # mart_sales_by_ticket, mart_sales_by_group
            └── order_times/

    """

    data_root: Path
    sucursales_json: Path

    @classmethod
    def from_root(
        cls,
        data_root: str | Path,
        sucursales_json: str | Path,
    ) -> DataPaths:
        """Create DataPaths from root directory and sucursales file.

        Args:
            data_root: Root directory for ETL data.
            sucursales_json: Path to sucursales.json configuration.

        Returns:
            DataPaths instance.

        Examples:
            >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
            >>> paths.data_root
            PosixPath('data')

        """
        if isinstance(data_root, str):
            data_root = Path(data_root)
        if isinstance(sucursales_json, str):
            sucursales_json = Path(sucursales_json)

        return cls(data_root=data_root, sucursales_json=sucursales_json)

    # Derived paths for payments
    @property
    def raw_payments(self) -> Path:
        """Bronze layer: raw payment Excel files."""
        return self.data_root / "a_raw" / "payments" / "batch"

    @property
    def clean_payments(self) -> Path:
        """Silver layer: fact_payments_ticket (cleaned CSVs)."""
        return self.data_root / "b_clean" / "payments" / "batch"

    @property
    def mart_payments(self) -> Path:
        """Gold layer: payment marts."""
        return self.data_root / "c_processed" / "payments"

    # Derived paths for sales
    @property
    def raw_sales(self) -> Path:
        """Bronze layer: raw sales Excel files."""
        return self.data_root / "a_raw" / "sales" / "batch"

    @property
    def clean_sales(self) -> Path:
        """Silver layer: fact_sales_item_line (cleaned CSVs)."""
        return self.data_root / "b_clean" / "sales" / "batch"

    @property
    def mart_sales(self) -> Path:
        """Gold layer: sales marts."""
        return self.data_root / "c_processed" / "sales"

    # Derived paths for order_times
    @property
    def raw_order_times(self) -> Path:
        """Bronze layer: raw order times Excel files."""
        return self.data_root / "a_raw" / "order_times" / "batch"

    @property
    def clean_order_times(self) -> Path:
        """Silver layer: order times core facts (cleaned CSVs)."""
        return self.data_root / "b_clean" / "order_times" / "batch"

    @property
    def mart_order_times(self) -> Path:
        """Gold layer: order times marts."""
        return self.data_root / "c_processed" / "order_times"

    # Derived paths for transfers
    @property
    def raw_transfers(self) -> Path:
        """Bronze layer: raw transfers Excel files."""
        return self.data_root / "a_raw" / "transfers" / "batch"

    @property
    def clean_transfers(self) -> Path:
        """Silver layer: fact_transfers_line (cleaned CSVs)."""
        return self.data_root / "b_clean" / "transfers" / "batch"

    @property
    def mart_transfers(self) -> Path:
        """Gold layer: transfers marts (pivot tables)."""
        return self.data_root / "c_processed" / "transfers"

    def ensure_dirs(self) -> None:
        """Create all directories in the data structure."""
        for path in [
            self.raw_payments,
            self.clean_payments,
            self.mart_payments,
            self.raw_sales,
            self.clean_sales,
            self.mart_sales,
            self.raw_order_times,
            self.clean_order_times,
            self.mart_order_times,
            self.raw_transfers,
            self.clean_transfers,
            self.mart_transfers,
        ]:
            path.mkdir(parents=True, exist_ok=True)
