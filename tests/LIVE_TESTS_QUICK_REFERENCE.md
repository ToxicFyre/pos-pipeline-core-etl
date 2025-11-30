# Live Tests Quick Reference

## Overview
6 comprehensive live tests across all test modules, using real POS credentials and data.

## Quick Commands

```bash
# Run all live tests
pytest -m live -v

# Run specific category
pytest tests/test_etl_smoke.py -m live -v         # ETL tests
pytest tests/test_etl_queries.py -m live -v       # Query tests  
pytest tests/test_qa_smoke.py -m live -v          # QA tests
pytest tests/test_forecasting_smoke.py -m live -v # Forecasting tests

# Skip live tests (CI/CD)
pytest -m "not live"

# Run all tests
pytest tests/
```

## Available Live Tests

| Test File | Test Name | What It Tests | Data Size |
|-----------|-----------|---------------|-----------|
| `test_forecasting_smoke.py` | `test_naive_forecasting_with_live_data` | Naive forecasting model + full pipeline | 45 days |
| `test_etl_smoke.py` | `test_etl_pipeline_with_live_data` | Complete ETL (download, clean, aggregate) | 14 days |
| `test_etl_queries.py` | `test_get_payments_with_live_data` | Query function + idempotence | 7 days |
| `test_etl_queries.py` | `test_get_payments_metadata_tracking` | Metadata tracking across stages | 5 days |
| `test_qa_smoke.py` | `test_qa_with_live_data` | QA checks at all levels (1-4) | 14 days |
| `test_qa_smoke.py` | `test_qa_detects_data_quality_issues` | Issue detection and reporting | 7 days |

## Prerequisites

Required environment variables:
- `WS_BASE` - POS API base URL
- `WS_USER` - POS username
- `WS_PASS` - POS password

Tests automatically skip if credentials not available.

## Test Results Summary

```
Total Tests: 16
├─ Regular Tests: 10 ✓
└─ Live Tests: 6 ✓

Execution Time: ~90 seconds (all tests)
```

## What Live Tests Validate

✅ Authentication with POS API  
✅ Data download (HTTP extraction)  
✅ ETL pipeline (all 3 stages)  
✅ Data quality & structure  
✅ Forecasting models  
✅ Query functions & caching  
✅ Metadata tracking  
✅ QA checks (levels 1-4)  
✅ Issue detection  

## Test Features

All live tests include:
- Graceful skipping if credentials unavailable
- Automatic environment variable cleaning
- Temporary directories (auto-cleanup)
- Comprehensive error handling
- Detailed progress logging
- Real data from Kavia branch (code: 8777)

## Example Output

### Forecasting Test
```
[Live Test] Downloaded 45 rows of payment data
[Live Test] Generated 7-day forecast:
2025-11-29    17402.0
2025-11-30    28093.6
...
[Live Test] ✓ Successfully validated naive forecasting
```

### ETL Test
```
[Live ETL Test] ETL completed successfully: 14 rows
[Live ETL Test] ✓ Validated 14 days of data for Kavia
[Live ETL Test] ✓ All data quality checks passed
```

### QA Test
```
[Live QA Test] Running QA checks at level 1...
[Live QA Test] Level 1 - Total rows: 14
...
[Live QA Test] ✓ Successfully validated QA pipeline
```

## Documentation

- [`README.md`](README.md) - Comprehensive test guide
- [`LIVE_TEST_SUMMARY.md`](LIVE_TEST_SUMMARY.md) - Detailed implementation summary
- This file - Quick reference

## Support

All tests use the same pattern:
1. Check credentials
2. Create temp directory
3. Download real data
4. Run test
5. Validate results
6. Auto-cleanup

If a test fails, check:
- Environment variables are set
- Network connectivity
- POS API availability
- Branch configuration (Kavia/8777)
