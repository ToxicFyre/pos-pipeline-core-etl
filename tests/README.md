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

#### Example: Naive Forecasting Live Test

The test `test_naive_forecasting_with_live_data` in `test_forecasting_smoke.py`:

1. **Downloads** 45 days of real payment data from the POS API (Kavia branch)
2. **Validates** data structure and content
3. **Runs** the naive forecasting model directly
4. **Tests** the full forecasting pipeline
5. **Verifies** the forecast results are reasonable

The test will:
- Skip if credentials are not available
- Clean up all downloaded data after completion (uses temporary directories)
- Print progress information for debugging

#### What It Validates

✅ Authentication with the POS API  
✅ Data download and ETL pipeline  
✅ Data quality and structure  
✅ Naive forecasting model produces valid predictions  
✅ Full forecasting pipeline integration  
✅ Forecast values are non-negative and reasonable  
✅ All expected metrics are forecasted (efectivo, credito, debito)  
✅ Metadata and results structure

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

## Test Organization

- `test_etl_*.py` - ETL pipeline tests
- `test_forecasting_*.py` - Forecasting model tests
- `test_qa_*.py` - QA functionality tests

Each test file may contain both regular and live tests.
