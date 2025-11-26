# Phase 2 – ETL Public API with Explicit `data_root`

## Overview
Refactor the payments ETL pipeline to expose a clean public API that accepts a configurable `data_root` path and uses direct function calls instead of subprocess orchestration. All core functions must be pure (no side effects), fully typed, and have clear error handling.

## Implementation Steps

### 1. Create `src/pos_core/etl/branch_config.py`
Create a new module for shared branch configuration utilities to avoid circular imports:
- Move `CodeWindow` dataclass from `build_payments_dataset.py` with full type hints
- Move `load_branch_segments_from_json()` function from `build_payments_dataset.py` with full type hints and docstrings
- Both `api.py` and `HTTP_extraction.py` will import from this module
- No side effects: pure functions that read JSON and return data structures

### 2. Create `src/pos_core/etl/api.py`
Create a new API module with:
- `PaymentsPaths` dataclass: holds all filesystem paths (raw_payments, clean_payments, proc_payments, sucursales_json) with full type hints (`Path` for all fields)
- `PaymentsETLConfig` dataclass: configuration with paths, chunk_size_days, excluded_branches, and a `from_data_root()` classmethod that builds default paths using the existing directory convention. All fields must have type hints.
- `ensure_dirs()` function: creates all required directories. Type signature: `ensure_dirs(config: PaymentsETLConfig) -> None`
- `build_payments_dataset()` function: main orchestration function that:
  - Type signature: `build_payments_dataset(start_date: str, end_date: str, config: PaymentsETLConfig, branches: Optional[List[str]] = None, steps: Optional[List[str]] = None) -> pd.DataFrame`
  - **No side effects**: no CLI parsing, no environment variable reading, no print statements
  - **Logging**: Create module-level `logger = logging.getLogger(__name__)` and use `logger.info/debug/warning` instead of print
  - **Clear error behavior**: raises `FileNotFoundError` with clear message if aggregated file is missing
  - Full type hints and docstring describing inputs, outputs, and behavior
  - Imports using `pos_core.etl` namespace (e.g., `from pos_core.etl.a_extract.HTTP_extraction import download_payments_reports`)
  - Calls `ensure_dirs(config)` first
  - Orchestrates extract → transform → aggregate steps
  - Supports selective step execution via `steps` parameter (default: all steps)
  - Returns the final aggregated DataFrame

### 3. Refactor `src/pos_core/etl/a_extract/HTTP_extraction.py`
Add a new function `download_payments_reports()` that:
- **Type signature**: `download_payments_reports(start_date: str, end_date: str, output_dir: Path, sucursales_json: Path, branches: Optional[List[str]] = None, chunk_size_days: int = 180) -> None`
- **Path handling**: Convert string paths to `Path` at function boundary if needed; inside function, assume `Path`
- **No side effects**: No CLI parsing, no environment variable reading, no print statements
- **Logging**: Create module-level `logger = logging.getLogger(__name__)` and use `logger.info/debug/warning` instead of print
- Imports `load_branch_segments_from_json` from `pos_core.etl.branch_config` (not from api.py to avoid circular import)
- Loads branch segments from sucursales.json
- Discovers existing intervals to skip already-downloaded ranges
- Iterates through branches and code windows
- Calls `export_sales_report()` for each missing chunk
- Handles chunking date ranges and directory creation
- **Docstring**: Describe expected inputs, path expectations, and behavior
- Keep existing CLI (`if __name__ == "__main__"`) for backward compatibility, but CLI must call this function
- Reuse existing functions: `export_sales_report()`, `make_session()`, `login_if_needed()`, etc.

### 4. Refactor `src/pos_core/etl/b_transform/pos_excel_payments_cleaner.py`
Add a new function `clean_payments_directory()` that:
- **Type signature**: `clean_payments_directory(input_dir: Path, output_dir: Path, recursive: bool = True) -> None`
- **Path handling**: Convert string paths to `Path` at function boundary if needed; inside function, assume `Path`
- **No side effects**: No CLI parsing, no environment variable reading, no print statements
- **Logging**: Create module-level `logger = logging.getLogger(__name__)` and use `logger.info/debug/warning` instead of print
- Iterates through Excel files in input_dir (recursively if recursive=True)
- Calls `transform_detalle_por_forma_pago()` for each file
- Writes cleaned CSVs to output_dir
- Handles sucursal inference from directory structure
- **Docstring**: Describe that it reads all .xlsx files under input_dir (recursively if recursive=True), writes normalized CSVs to output_dir
- Keep existing CLI (`if __name__ == "__main__"`) for backward compatibility, but CLI must call this function
- Reuse existing functions: `transform_detalle_por_forma_pago()`, `iter_xlsx_files()`, etc.

### 5. Refactor `src/pos_core/etl/c_load/aggregate_payments_by_day.py`
Add a new function `aggregate_payments_daily()` that:
- **Type signature**: `aggregate_payments_daily(clean_dir: Path, output_path: Path) -> pd.DataFrame`
- **Path handling**: Convert string paths to `Path` at function boundary if needed; inside function, assume `Path`
- **No side effects**: No CLI parsing, no environment variable reading, no print statements
- **Logging**: Create module-level `logger = logging.getLogger(__name__)` and use `logger.info/debug/warning` instead of print
- Reads all CSV files from clean_dir (recursively)
- Calls `aggregate_payments()` to perform aggregation
- Writes result to output_path
- Returns the aggregated DataFrame
- **Error handling**: Raises `FileNotFoundError` or `ValueError` with clear message if no cleaned CSV files are found
- **Docstring**: Describe that it reads all clean CSV files from clean_dir, aggregates to daily level, writes to output_path, returns DataFrame
- Keep existing CLI (`if __name__ == "__main__"`) for backward compatibility, but CLI must call this function
- Reuse existing functions: `aggregate_payments()`, `read_clean_csv()`, `iter_csv_files()`, etc.

### 6. Update `src/pos_core/etl/build_payments_dataset.py`
Refactor the CLI entry point to:
- Remove all `subprocess.run()` calls
- Import and use `build_payments_dataset()` from `pos_core.etl.api`
- Keep the same CLI arguments but pass them to the new API
- Use `PaymentsETLConfig.from_data_root()` to build config
- Add `--data-root` argument (default: "data")
- **All CLI parsing, environment variable reading, and print statements stay ONLY in this file under `if __name__ == "__main__"`**
- The main `build_payments_dataset()` function in this file should be removed or become a thin wrapper that calls the API version

### 7. Update `src/pos_core/etl/__init__.py`
Export the new API:
- Import `PaymentsPaths`, `PaymentsETLConfig`, `build_payments_dataset` from `.api`
- Add to `__all__` list

### 8. Create smoke test
Create `tests/test_etl_smoke.py` with:
- `from pos_core.etl import PaymentsETLConfig, build_payments_dataset`
- Construct `PaymentsETLConfig.from_data_root(Path("data"))`
- Assert `callable(build_payments_dataset)`
- No need to actually run the ETL in the smoke test yet

### 9. Sanity Check
- Run `python -m compileall src` to check for import errors
- Verify imports work: `from pos_core.etl import PaymentsETLConfig, build_payments_dataset`
- Test config creation: `PaymentsETLConfig.from_data_root(Path("data"))`

## Key Design Constraints

1. **No Side Effects in Core Functions**: All core functions (`download_payments_reports`, `clean_payments_directory`, `aggregate_payments_daily`, `build_payments_dataset`) must not parse CLI args, read environment variables, or print to stdout (use `logging` only)
2. **Full Type Hints**: All functions and dataclasses must have complete type annotations
3. **Consistent Path Usage**: Use `pathlib.Path` consistently; convert strings to `Path` at function boundaries
4. **Clear Error Behavior**: Raise explicit exceptions (`FileNotFoundError`, `ValueError`) with clear messages when expected files/data are missing
5. **Import Structure**: Use `pos_core.etl` namespace for all imports (no relative paths with `..`)
6. **Backward Compatibility**: Keep all existing CLI entry points working by preserving `if __name__ == "__main__"` blocks
7. **Avoid Circular Imports**: Put shared utilities (`CodeWindow`, `load_branch_segments_from_json`) in `branch_config.py` to avoid circular imports between `api.py` and `HTTP_extraction.py`
8. **Logging Pattern**: All modules should create a module-level `logger = logging.getLogger(__name__)` and use `logger.info/debug/warning` instead of print statements

## Files to Modify

1. **Create**: `src/pos_core/etl/branch_config.py` (new file - to avoid circular imports)
2. **Create**: `src/pos_core/etl/api.py` (new file)
3. **Modify**: `src/pos_core/etl/a_extract/HTTP_extraction.py` (add `download_payments_reports()`)
4. **Modify**: `src/pos_core/etl/b_transform/pos_excel_payments_cleaner.py` (add `clean_payments_directory()`)
5. **Modify**: `src/pos_core/etl/c_load/aggregate_payments_by_day.py` (add `aggregate_payments_daily()`)
6. **Modify**: `src/pos_core/etl/build_payments_dataset.py` (refactor to use new API)
7. **Modify**: `src/pos_core/etl/__init__.py` (export new API)
8. **Create**: `tests/test_etl_smoke.py` (smoke test)

