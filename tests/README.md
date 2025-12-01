# Testing Guide

This directory contains tests for the pos-core-etl package.

## Test Types

### Unit/Smoke Tests

Regular tests that run quickly with synthetic data and no external dependencies:

```bash
pytest tests/
```

### Live Tests

Live tests use real credentials to download actual data and validate the system end-to-end. These tests are marked with `@pytest.mark.live` and can be run separately.

#### Running Live Tests

To run **only** live tests:

```bash
pytest -m live
```

To run all tests **except** live tests (default for CI/CD):

```bash
pytest -m "not live"
```

#### Prerequisites for Live Tests

Live tests require the following environment variables:

- `WS_BASE`: POS API base URL
- `WS_USER`: POS username  
- `WS_PASS`: POS password

These should be set in your environment or CI/CD secrets.

#### Available Live Tests

**1. Forecasting Tests** (`test_forecasting_smoke.py`)

- `test_naive_forecasting_with_live_data`: Downloads 45 days of data, tests naive forecasting model and full pipeline

**2. ETL Tests** (`test_etl_smoke.py`)

- `test_etl_pipeline_with_live_data`: Tests complete ETL pipeline (download, clean, aggregate) with 14 days of real data

**3. Query Tests** (`test_etl_queries.py`)

- `test_get_payments_with_live_data`: Tests payments ETL pipeline with idempotence validation
- `test_get_payments_metadata_tracking`: Verifies metadata tracking throughout ETL stages

**4. QA Tests** (`test_qa_smoke.py`)

- `test_qa_with_live_data`: Runs QA checks at all levels (1-4) on real data
- `test_qa_detects_data_quality_issues`: Validates QA issue detection capabilities

#### What Live Tests Validate

✅ **Authentication**: With the POS API  
✅ **Data Download**: HTTP extraction with real credentials  
✅ **ETL Pipeline**: Download, clean, and aggregate stages  
✅ **Data Quality**: Structure, completeness, and validity  
✅ **Forecasting**: Naive model and full pipeline integration  
✅ **Query Functions**: Idempotence and caching behavior  
✅ **Metadata**: Tracking and version control  
✅ **QA Checks**: Issue detection and reporting at all levels  

All live tests:
- Skip gracefully if credentials are not available
- Use temporary directories (auto-cleanup)
- Print detailed progress information
- Validate results comprehensively

## Running Specific Tests

Run a specific test file:
```bash
pytest tests/test_forecasting_smoke.py -v
```

Run a specific test function:
```bash
pytest tests/test_forecasting_smoke.py::test_naive_forecasting_with_live_data -v
```

Run with output (show print statements):
```bash
pytest tests/test_forecasting_smoke.py::test_naive_forecasting_with_live_data -v -s
```

Run all live tests:
```bash
pytest -m live -v
```

Run specific live test category:
```bash
pytest tests/test_etl_smoke.py -m live -v     # ETL live tests
pytest tests/test_qa_smoke.py -m live -v      # QA live tests
pytest tests/test_etl_queries.py -m live -v   # Query live tests
```

## Test Organization

- `test_etl_*.py` - ETL pipeline tests
- `test_forecasting_*.py` - Forecasting model tests
- `test_qa_*.py` - QA functionality tests

Each test file may contain both regular and live tests.

## Additional Documentation

- [`LIVE_TESTS_QUICK_REFERENCE.md`](LIVE_TESTS_QUICK_REFERENCE.md) - Quick reference for live tests
- [`LIVE_TEST_SUMMARY.md`](LIVE_TEST_SUMMARY.md) - Detailed implementation summary of live tests
