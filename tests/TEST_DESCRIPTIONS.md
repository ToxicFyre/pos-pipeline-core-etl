# Test Descriptions - Complete Overview

This document describes what each test in the test suite is testing for.

---

## Test File: `test_etl_smoke.py`

**Purpose**: Smoke tests for ETL API imports and basic functionality

### Regular Tests (5 tests)

1. **`test_imports_work`**
   - **Tests**: Verifies that the new domain-oriented ETL API can be imported without errors
   - **Validates**: DataPaths, payments_core.fetch, payments_marts.fetch_daily, sales_core.fetch, sales_marts.fetch_ticket are all importable

2. **`test_config_creation`**
   - **Tests**: Verifies that DataPaths can be created from data_root and sucursales_json paths
   - **Validates**: DataPaths object creation and derived path properties (raw_payments, clean_payments, mart_payments, raw_sales, clean_sales, mart_sales)

3. **`test_payments_fetch_is_callable`**
   - **Tests**: Verifies that payments.core.fetch is a callable function
   - **Validates**: Function exists and can be called

4. **`test_sales_fetch_is_callable`**
   - **Tests**: Verifies that sales.core.fetch is a callable function
   - **Validates**: Function exists and can be called

5. **`test_payments_fetch_invalid_mode`**
   - **Tests**: Verifies that payments.core.fetch raises ValueError for invalid mode parameter
   - **Validates**: Error handling for invalid input

6. **`test_sales_fetch_invalid_mode`**
   - **Tests**: Verifies that sales.core.fetch raises ValueError for invalid mode parameter
   - **Validates**: Error handling for invalid input

### Live Test (1 test)

7. **`test_etl_pipeline_with_live_data`** ⭐ LIVE TEST
   - **Tests**: Full ETL pipeline end-to-end with real POS data
   - **Uses**: Real credentials (WS_BASE, WS_USER, WS_PASS)
   - **Downloads**: 14 days of real payment data
   - **Validates**:
     - Authentication with POS API
     - Data download from POS API
     - ETL pipeline execution (download → clean → aggregate)
     - Result DataFrame is not empty
     - Required columns exist (sucursal, fecha, ingreso_efectivo, ingreso_credito, ingreso_debito)
     - Kavia branch data is present
     - Numeric columns are non-negative
   - **Fails if**: Credentials provided but authentication fails, data download fails, or data is empty

---

## Test File: `test_forecasting_smoke.py`

**Purpose**: Smoke tests for forecasting API imports and functionality

### Regular Tests (5 tests)

1. **`test_forecasting_smoke`**
   - **Tests**: Basic forecasting functionality with synthetic data
   - **Uses**: 40 days of synthetic payment data for one branch
   - **Validates**:
     - ForecastResult structure
     - Forecast DataFrame is not empty
     - Deposit schedule DataFrame is not empty
     - Required columns exist (sucursal, fecha, metric, valor)
     - Metadata fields are present

2. **`test_run_payments_forecast_exposes_debug_info`**
   - **Tests**: Debug info exposure when debug=True
   - **Uses**: Synthetic data with NaiveLastWeekModel
   - **Validates**:
     - Debug info is populated when debug=True
     - Debug info structure (model_name, branch, metric)
     - Source dates mapping exists for naive model
     - Debug info exists for all forecasted metrics

3. **`test_run_payments_forecast_no_debug_by_default`**
   - **Tests**: Debug info is None by default (debug=False)
   - **Uses**: Synthetic data
   - **Validates**: Debug info is None when debug flag is not set

4. **`test_debug_info_tracks_multiple_branches_and_metrics`**
   - **Tests**: Debug info tracking for multiple branches and metrics
   - **Uses**: Synthetic data for two branches (Kavia, CrediClub)
   - **Validates**:
     - Debug info exists for both branches
     - Debug info exists for multiple metrics
     - Each branch/metric combination has its own debug info
     - Source dates mappings differ between branches

5. **`test_imports_work`**
   - **Tests**: Verifies forecasting API can be imported
   - **Validates**: ForecastConfig, ForecastResult, run_payments_forecast are importable

### Live Test (1 test)

6. **`test_naive_forecasting_with_live_data`** ⭐ LIVE TEST
   - **Tests**: Naive forecasting model with real POS data
   - **Uses**: Real credentials (WS_BASE, WS_USER, WS_PASS)
   - **Downloads**: 45 days of real payment data
   - **Validates**:
     - Authentication with POS API
     - Data download from POS API
     - Naive model training with real data
     - 7-day forecast generation
     - Forecast values are non-negative and non-NaN
     - Debug info structure for naive model
     - Source dates mapping correctness
     - Full forecasting pipeline integration
     - Forecast structure (sucursal, fecha, metric, valor)
     - Deposit schedule generation
     - All payment metrics are forecasted (efectivo, credito, debito)
   - **Fails if**: Credentials provided but authentication fails, data download fails, or data is empty

---

## Test File: `test_qa_smoke.py`

**Purpose**: Smoke tests for QA API imports and functionality

### Regular Tests (2 tests)

1. **`test_qa_imports`**
   - **Tests**: Verifies QA API can be imported
   - **Validates**: PaymentsQAResult, run_payments_qa are importable

2. **`test_run_payments_qa_basic`**
   - **Tests**: Basic QA functionality with minimal synthetic data
   - **Uses**: Synthetic DataFrame with 3 rows, 2 branches
   - **Validates**:
     - Returns PaymentsQAResult instance
     - Summary dictionary structure
     - Total rows and sucursales counts

### Live Tests (2 tests)

3. **`test_qa_with_live_data`** ⭐ LIVE TEST
   - **Tests**: QA checks on real POS data at all levels
   - **Uses**: Real credentials (WS_BASE, WS_USER, WS_PASS)
   - **Downloads**: 14 days of real payment data
   - **Validates**:
     - Authentication with POS API
     - Data download from POS API
     - QA checks at levels 1, 2, 3, and 4
     - QA result structure for each level
     - Summary fields (total_rows, total_sucursales)
     - Data completeness
   - **Fails if**: Credentials provided but authentication fails, data download fails, or data is empty

4. **`test_qa_detects_data_quality_issues`** ⭐ LIVE TEST
   - **Tests**: QA issue detection capabilities with real data
   - **Uses**: Real credentials (WS_BASE, WS_USER, WS_PASS)
   - **Downloads**: 7 days of real payment data
   - **Validates**:
     - Authentication with POS API
     - Data download from POS API
     - Comprehensive QA (level 4) execution
     - QA result structure
     - Data completeness validation
     - Issue detection functionality
   - **Fails if**: Credentials provided but authentication fails, data download fails, or data is empty

---

## Test File: `test_etl_queries.py`

**Purpose**: High-level tests for ETL query functions

### Regular Tests (2 tests)

1. **`test_get_sales_refresh_runs_all_stages`**
   - **Tests**: Refresh mode runs all ETL stages
   - **Uses**: Mocked download and clean functions
   - **Validates**:
     - Download stage is called when mode="force"
     - Clean stage is called when mode="force"
     - Metadata is written after each stage
     - Result is returned

2. **`test_get_sales_uses_existing_data_when_refresh_false`**
   - **Tests**: Caching behavior when refresh=False
   - **Uses**: Pre-created clean files and metadata
   - **Validates**:
     - Existing data is used when mode="missing"
     - No redundant processing occurs
     - Result matches existing data

### Live Tests (2 tests)

3. **`test_get_payments_with_live_data`** ⭐ LIVE TEST
   - **Tests**: Payments ETL pipeline with idempotence validation
   - **Uses**: Real credentials (WS_BASE, WS_USER, WS_PASS)
   - **Downloads**: 7 days of real payment data
   - **Validates**:
     - Authentication with POS API
     - Data download from POS API
     - First call downloads and processes data
     - Second call with mode="missing" uses cached data
     - Idempotence (both calls return same data)
     - Data quality (columns, structure)
     - Kavia branch data presence
     - Numeric columns are non-negative
   - **Fails if**: Credentials provided but authentication fails, data download fails, or data is empty

4. **`test_get_payments_metadata_tracking`** ⭐ LIVE TEST
   - **Tests**: Metadata tracking throughout ETL stages
   - **Uses**: Real credentials (WS_BASE, WS_USER, WS_PASS)
   - **Downloads**: 5 days of real payment data
   - **Validates**:
     - Authentication with POS API
     - Data download from POS API
     - Metadata is written for raw stage
     - Metadata is written for clean stage
     - Metadata is written for mart stage
     - All metadata status is "ok"
     - Metadata prevents redundant processing
   - **Fails if**: Credentials provided but authentication fails, data download fails, or data is empty

---

## Test File: `test_grain_assertions.py`

**Purpose**: Tests for data grain assertions

### Regular Tests (8 tests)

1. **`test_sales_item_line_grain_has_multiple_rows_per_ticket`**
   - **Tests**: Sales data grain allows multiple rows per ticket
   - **Uses**: Synthetic sales data
   - **Validates**: Multiple item-lines per order_id (ticket)

2. **`test_sales_item_line_key_uniqueness`**
   - **Tests**: Item-line key uniquely identifies rows
   - **Uses**: Synthetic sales data
   - **Validates**: No duplicate keys (sucursal, operating_date, order_id, item_key)

3. **`test_sales_item_line_has_required_columns`**
   - **Tests**: Item-line data has all required columns
   - **Uses**: Synthetic sales data
   - **Validates**: Required columns exist (sucursal, operating_date, order_id, item_key, group, subtotal_item, total_item)

4. **`test_sales_aggregation_to_ticket_is_mart`**
   - **Tests**: Aggregating from item-line to ticket is a mart operation
   - **Uses**: Synthetic sales data
   - **Validates**: Aggregation reduces row count, totals are correct

5. **`test_payments_ticket_grain_allows_multiple_payment_methods`**
   - **Tests**: Payments data allows multiple rows per ticket (split payments)
   - **Uses**: Synthetic payments data
   - **Validates**: Multiple payment methods per order_index (ticket)

6. **`test_payments_ticket_key_uniqueness`**
   - **Tests**: Ticket x payment method key uniquely identifies rows
   - **Uses**: Synthetic payments data
   - **Validates**: No duplicate keys (sucursal, operating_date, order_index, payment_method)

7. **`test_payments_ticket_has_required_columns`**
   - **Tests**: Payment data has all required columns
   - **Uses**: Synthetic payments data
   - **Validates**: Required columns exist (sucursal, operating_date, order_index, payment_method, ticket_total)

8. **`test_payments_aggregation_to_daily_is_mart`**
   - **Tests**: Aggregating from ticket to daily is a mart operation
   - **Uses**: Synthetic payments data
   - **Validates**: Aggregation reduces row count, totals are correct

9. **`test_payments_module_documents_grain`**
   - **Tests**: Payments module documents grain in docstring
   - **Validates**: Docstring mentions ticket grain and fact_payments_ticket

10. **`test_sales_module_documents_grain`**
    - **Tests**: Sales module documents grain in docstring
    - **Validates**: Docstring mentions item grain and fact_sales_item_line

11. **`test_paths_documents_layers`**
    - **Tests**: Paths module documents data layers
    - **Validates**: Docstring mentions raw/bronze, clean/silver, mart/gold layers

12. **`test_payments_core_importable`**
    - **Tests**: Payments.core can be imported
    - **Validates**: core.fetch and core.load are callable

13. **`test_sales_core_importable`**
    - **Tests**: Sales.core can be imported
    - **Validates**: core.fetch and core.load are callable

14. **`test_data_paths_importable`**
    - **Tests**: DataPaths can be imported
    - **Validates**: DataPaths class exists

15. **`test_forecasting_api_importable`**
    - **Tests**: Forecasting API can be imported
    - **Validates**: ForecastConfig, ForecastResult, run_payments_forecast are importable

16. **`test_qa_api_importable`**
    - **Tests**: QA API can be imported
    - **Validates**: PaymentsQAResult, run_payments_qa are importable

---

## Test File: `test_sales_by_ticket.py`

**Purpose**: Tests for sales_by_ticket aggregation module

### Regular Tests (7 tests)

1. **`test_aggregate_by_ticket_with_recursive_directory`**
   - **Tests**: Recursive directory search handling
   - **Uses**: Synthetic data in nested directories
   - **Validates**: Files in subdirectories are found and aggregated

2. **`test_aggregate_by_ticket_filters_out_directories`**
   - **Tests**: Directories are filtered out from file lists
   - **Uses**: Synthetic data with directories
   - **Validates**: Only CSV files are read, directories are ignored

3. **`test_aggregate_by_ticket_with_mixed_paths`**
   - **Tests**: Mixed file and directory paths handling
   - **Uses**: Synthetic data in multiple locations
   - **Validates**: All sources are aggregated correctly

4. **`test_aggregate_by_ticket_with_empty_directory`**
   - **Tests**: Error handling for empty directory
   - **Uses**: Empty directory
   - **Validates**: Raises FileNotFoundError with appropriate message

5. **`test_aggregate_by_ticket_with_directory_no_csv_files`**
   - **Tests**: Error handling for directory with no CSV files
   - **Uses**: Directory with non-CSV files
   - **Validates**: Raises FileNotFoundError with appropriate message

6. **`test_aggregate_by_ticket_with_file_path`**
   - **Tests**: Direct file path handling
   - **Uses**: Single CSV file
   - **Validates**: File is read and aggregated correctly

7. **`test_aggregate_by_ticket_with_glob_pattern`**
   - **Tests**: Glob pattern handling
   - **Uses**: Multiple CSV files matching pattern
   - **Validates**: All matching files are aggregated

### Live Tests (2 tests)

8. **`test_aggregate_by_ticket_with_directory_path_live`** ⭐ LIVE TEST
   - **Tests**: Directory path handling fix (PermissionError bug)
   - **Uses**: Real credentials (WS_BASE, WS_USER, WS_PASS)
   - **Downloads**: 2 days of real sales data
   - **Validates**:
     - Authentication with POS API
     - Data download from POS API
     - Directory path handling (no PermissionError)
     - Ticket aggregation works correctly
     - Result DataFrame is not empty
     - Required columns exist (order_id, sucursal)
     - Output file is created
   - **Fails if**: Credentials provided but authentication fails, data download fails, or data is empty

9. **`test_fetch_group_date_filtering_edge_case`** ⭐ LIVE TEST
   - **Tests**: Date filtering edge case (non-overlapping ranges)
   - **Uses**: Real credentials (WS_BASE, WS_USER, WS_PASS)
   - **Downloads**: Two non-overlapping date ranges (7 days each)
   - **Validates**:
     - Authentication with POS API
     - Data download from POS API
     - Range A processing succeeds
     - Range B processing succeeds
     - Date ranges don't overlap
     - Range A only contains dates from range A
     - Range B only contains dates from range B
     - Mart files are created correctly
   - **Fails if**: Credentials provided but authentication fails, data download fails, or data is empty

---

## Summary Statistics

- **Total Tests**: 37
  - **Regular Tests**: 29 (use synthetic data, no credentials)
  - **Live Tests**: 8 (use real credentials and real data)

- **Test Categories**:
  - **ETL Tests**: 6 (1 live)
  - **Forecasting Tests**: 6 (1 live)
  - **QA Tests**: 4 (2 live)
  - **Query Tests**: 4 (2 live)
  - **Grain Assertion Tests**: 16 (0 live)
  - **Sales Aggregation Tests**: 9 (2 live)

- **Live Test Requirements**:
  - All require: WS_BASE, WS_USER, WS_PASS environment variables
  - All skip gracefully if credentials are missing
  - All **FAIL** if credentials are provided but authentication/data retrieval fails
  - All validate data structure and quality

---

## Key Testing Principles

1. **No Cheating**: Live tests always use real credentials and real data
2. **Fail Appropriately**: Live tests fail (not skip) when credentials are provided but requirements aren't met
3. **Comprehensive Validation**: All live tests validate data structure, quality, and completeness
4. **Clear Separation**: Regular tests use synthetic data, live tests use real data
5. **Graceful Skipping**: Live tests skip only when credentials are completely missing

---

**Last Updated**: 2025-01-XX
