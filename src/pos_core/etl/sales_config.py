"""Sales ETL configuration.

This module provides configuration dataclasses for the sales ETL pipeline,
mirroring the structure of PaymentsETLConfig for consistency.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class SalesPaths:
    """All filesystem paths used by the sales ETL.

    Attributes:
        raw_sales: Directory for raw sales Excel files (e.g., data/a_raw/sales/batch).
        clean_sales: Directory for cleaned sales CSV files (e.g., data/b_clean/sales/batch).
        proc_sales: Directory for processed/aggregated sales data
            (e.g., data/c_processed/sales).
        sucursales_json: Path to sucursales.json configuration file.
    """

    raw_sales: Path
    clean_sales: Path
    proc_sales: Path
    sucursales_json: Path


@dataclass
class SalesETLConfig:
    """Configuration for the sales ETL pipeline.

    Attributes:
        paths: All filesystem paths used by the pipeline.
        chunk_days: Maximum number of days per HTTP request chunk (default: 180).
            Only used if chunking is needed for sales extraction.
    """

    paths: SalesPaths
    chunk_days: int = 180

    @classmethod
    def from_root(
        cls,
        data_root: str | Path,
        sucursales_file: str | Path,
    ) -> SalesETLConfig:
        """Build a default config given a data_root and sucursales file.

        Uses the existing directory structure:
          data_root/a_raw/sales/batch
          data_root/b_clean/sales/batch
          data_root/c_processed/sales

        Args:
            data_root: Root directory for ETL data (will be converted to Path if string).
            sucursales_file: Path to sucursales.json configuration file
                (will be converted to Path if string).

        Returns:
            SalesETLConfig instance with default paths.

        Examples:
            >>> from pathlib import Path
            >>> config = SalesETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
            >>> config.paths.raw_sales
            PosixPath('data/a_raw/sales/batch')
        """
        # Convert string to Path if needed
        if isinstance(data_root, str):
            data_root = Path(data_root)
        if isinstance(sucursales_file, str):
            sucursales_file = Path(sucursales_file)

        paths = SalesPaths(
            raw_sales=data_root / "a_raw" / "sales" / "batch",
            clean_sales=data_root / "b_clean" / "sales" / "batch",
            proc_sales=data_root / "c_processed" / "sales",
            sucursales_json=sucursales_file,
        )

        return cls(paths=paths)
