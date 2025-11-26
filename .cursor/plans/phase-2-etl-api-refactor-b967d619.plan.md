<!-- b967d619-bc10-4c16-9aa5-4f2788b981a8 46b07820-af4c-4d90-acae-33196ea5c02d -->
# Phase 2 – ETL public API with explicit data_root

## Overview

Refactor the payments ETL pipeline to expose a clean public API that:

- Accepts a `data_root` path parameter instead of hardcoding `data/`
- Uses config objects for all paths
- Calls existing extract/clean/aggregate logic directly (no subprocess)
- Returns the final aggregated payments DataFrame

## Implementation Steps

### 1. Create `src/pos_core/etl/api.py`

Create a new API module with:

- `PaymentsPaths` dataclass: holds all filesystem paths (raw_payments, clean_payments, proc_payments, sucursales_json)
- All fields typed as `Path`
- Full type hints and docstrings
- `PaymentsETLConfig` dataclass: configuration with paths, chunk_size_days, excluded_branches
- Full type hints and docstrings
- `PaymentsETLConfig.from_data_root()`: factory method to build default config from data_root
- Accepts `data_root: Path`, converts string to Path if needed
- Returns `PaymentsETLConfig` with proper typing
- `ensure_dirs()`: helper to create all required directories
- Type: `(config: PaymentsETLConfig) -> None`
- No side effects except directory creation
- `build_payments_dataset()`: main orchestration function that:
- Takes `start_date: str`, `end_date: str`, `config: PaymentsETLConfig`, optional `branches: Optional[List[str]]`, `steps: Optional[List[str]]`
- Returns `pd.DataFrame` (the aggregated payments dataset)
- **No side effects**: no CLI args, no env vars, no stdout printing (use logging only)
- **Clear error behavior**: raises `FileNotFoundError` if aggregated file is missing
- Full type hints and docstring describing inputs, outputs, and behavior
- Imports using `pos_core.etl` namespace (e.g., `from pos_core.etl.a_extract.HTTP_extraction import download_payments_reports`)

### 2. Refactor `src/pos_core/etl/a_extract/HTTP_extraction.py`

Extract reusable function for payments extraction:

- Add `download_payments_reports()` function that:
- Takes start_date, end_date, output_dir, sucursales_json, optional branches, chunk_size_days
- Loads branch segments from sucursales.json
- Discovers existing intervals to skip already-downloaded ranges
- Iterates through branches and code windows
- Calls `export_sales_report()` for each missing chunk
- Handles chunking date ranges and directory creation
- Keep existing CLI (`if __name__ == "__main__"`) for backward compatibility
- Reuse existing functions: `export_sales_report()`, `make_session()`, `login_if_needed()`, etc.

### 3. Refactor `src/pos_core/etl/b_transform/pos_excel_payments_cleaner.py`

Extract reusable function for payments cleaning:

- Add `clean_payments_directory()` function that:
- Takes input_dir, output_dir, optional recursive flag
- Iterates through Excel files in input_dir
- Calls `transform_detalle_por_forma_pago()` for each file
- Writes cleaned CSVs to output_dir
- Handles sucursal inference from directory structure
- Keep existing CLI (`if __name__ == "__main__"`) for backward compatibility
- Reuse existing functions: `transform_detalle_por_forma_pago()`, `iter_xlsx_files()`, etc.

### 4. Refactor `src/pos_core/etl/c_load/aggregate_payments_by_day.py`

Extract reusable function for payments aggregation:

- Add `aggregate_payments_daily()` function that:
- Takes clean_dir, output_path
- Reads all CSV files from clean_dir (recursively)
- Calls `aggregate_payments()` to perform aggregation
- Writes result to output_path
- Returns the aggregated DataFrame
- Keep existing CLI (`if __name__ == "__main__"`) for backward compatibility
- Reuse existing functions: `aggregate_payments()`, `read_clean_csv()`, `iter_csv_files()`, etc.

### 5. Implement `build_payments_dataset()` in `api.py`

Complete the orchestration function:

- Import helper functions from the three modules above
- Call `ensure_dirs(config)` first
- Support optional `steps` parameter to run only specific stages
- For each step:
- **extract**: Call `download_payments_reports()` with config paths
- **transform**: Call `clean_payments_directory()` with config paths
- **aggregate**: Call `aggregate_payments_daily()` with config paths
- Load and return the final aggregated DataFrame (or raise if missing)

### 6. Update `src/pos_core/etl/build_payments_dataset.py`

Refactor the existing CLI module:

- Remove all subprocess orchestration logic
- Import `PaymentsETLConfig` and `build_payments_dataset` from `pos_core.etl.api`
- Update CLI to:
- Accept `--data-root` argument (default: "data")
- Build config using `PaymentsETLConfig.from_data_root()`
- Call `build_payments_dataset()` with the config
- Keep existing CLI arguments for backward compatibility (start, end, max-days-per-chunk, etc.)
- Remove `run_cmd()`, `load_branch_segments_from_json()`, and other orchestration code that's now in the API

### 7. Update `src/pos_core/etl/__init__.py`

Export the new API:

- Import `PaymentsPaths`, `PaymentsETLConfig`, `build_payments_dataset` from `.api`
- Add to `__all__` list

### 8. Sanity Check

- Run `python -m compileall src` to check for import errors
- Verify imports work: `from pos_core.etl import PaymentsETLConfig, build_payments_dataset`
- Test config creation: `PaymentsETLConfig.from_data_root(Path("data"))`

## Key Design Decisions

1. **Backward Compatibility**: Keep all existing CLI entry points working by preserving `if __name__ == "__main__"` blocks in all modules
2. **No Breaking Changes**: Existing scripts that call modules via subprocess will continue to work
3. **Path Convention**: Use the existing `a_raw/payments/batch`, `b_clean/payments/batch`, `c_processed/payments` structure, but make it configurable via `data_root`
4. **Function Extraction**: Extract the core logic into reusable functions while keeping CLI wrappers
5. **Config Objects**: Use dataclasses for type safety and clear API contracts

## Files to Modify

1. **Create**: `src/pos_core/etl/api.py`
2. **Modify**: `src/pos_core/etl/a_extract/HTTP_extraction.py`
3. **Modify**: `src/pos_core/etl/b_transform/pos_excel_payments_cleaner.py`
4. **Modify**: `src/pos_core/etl/c_load/aggregate_payments_by_day.py`
5. **Modify**: `src/pos_core/etl/build_payments_dataset.py`
6. **Modify**: `src/pos_core/etl/__init__.py`

## Dependencies

- Reuse existing utilities from `pos_core.etl.utils` (discover_existing_intervals, iter_chunks, etc.)
- Reuse existing config loading logic from `build_payments_dataset.py` (load_branch_segments_from_json)
- Maintain compatibility with existing `pos_core.etl.config` module (though we're moving away from hardcoded paths)

### To-dos

- [ ] Create src/pos_core/etl/api.py with PaymentsPaths, PaymentsETLConfig, ensure_dirs, and build_payments_dataset skeleton
- [ ] Add download_payments_reports() function to HTTP_extraction.py that handles branch segments, date chunking, and calls export_sales_report()
- [ ] Add clean_payments_directory() function to pos_excel_payments_cleaner.py that processes all Excel files in a directory
- [ ] Add aggregate_payments_daily() function to aggregate_payments_by_day.py that reads CSVs and returns DataFrame
- [ ] Complete build_payments_dataset() implementation in api.py to call the three helper functions and return aggregated DataFrame
- [ ] Refactor build_payments_dataset.py CLI to use the new API instead of subprocess calls
- [ ] Update src/pos_core/etl/__init__.py to export PaymentsPaths, PaymentsETLConfig, and build_payments_dataset
- [ ] Run compileall and verify imports work correctly