# Example Update: Auto-Download Data Feature

## Summary

Modified `examples/naive_forecast_example.py` to automatically download payments data using the ETL pipeline if the data file is not already present.

## Changes Made

### Before
- Example only worked if `aggregated_payments_daily.csv` already existed
- Would fall back to synthetic data immediately if file was missing
- User had to manually run ETL pipeline separately

### After
- **Automatically attempts to download data using ETL pipeline** if file is missing
- Checks for required configuration (`utils/sucursales.json`)
- Downloads last 90 days of data for quick demo
- Saves downloaded data for future runs
- Provides helpful error messages if configuration is missing
- Falls back to synthetic data only if ETL pipeline fails

## New Flow

```
1. Check if data/c_processed/payments/aggregated_payments_daily.csv exists
   ├─ YES: Load existing data and run forecast
   └─ NO: Attempt to build dataset
       ├─ Check if utils/sucursales.json exists
       │  ├─ YES: Run ETL pipeline
       │  │       ├─ Download data (last 90 days)
       │  │       ├─ Save to CSV
       │  │       └─ Run forecast
       │  └─ NO: Show helpful error message
       │         └─ Fall back to synthetic data
       └─ If ETL fails: Fall back to synthetic data
```

## Code Changes

### Added Imports
```python
from datetime import date, timedelta
from pos_core.etl import PaymentsETLConfig, build_payments_dataset
```

### Added ETL Pipeline Integration
```python
if data_file.exists():
    # Load existing data (original behavior)
    ...
else:
    # NEW: Attempt to build payments dataset using ETL pipeline
    try:
        # Set up ETL configuration
        sucursales_json = Path("utils/sucursales.json")
        
        if sucursales_json.exists():
            config = PaymentsETLConfig.from_data_root(data_root)
            
            # Download last 90 days of data
            end_date = date.today()
            start_date = end_date - timedelta(days=90)
            
            payments_df = build_payments_dataset(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                config=config,
            )
            
            # Save for future use
            payments_df.to_csv(data_file, index=False)
            
            # Run forecast with downloaded data
            ...
        else:
            # Show helpful error message
            ...
    except Exception as e:
        # Fall back to synthetic data
        ...
```

## Benefits

1. **Better User Experience**: Example works out-of-the-box if environment is configured
2. **Self-Documenting**: Shows how to use ETL pipeline programmatically
3. **Graceful Degradation**: Falls back to synthetic data if setup is incomplete
4. **Helpful Error Messages**: Guides user on what's missing
5. **Caches Data**: Downloads once and reuses on subsequent runs

## Prerequisites (for auto-download to work)

1. Set `WS_BASE` environment variable pointing to POS API
2. Create `utils/sucursales.json` with branch configuration
3. Optional: Set `WS_USER` and `WS_PASS` if authentication required

## Alternative Usage (without auto-download)

If you don't have access to POS API, the example will:
1. Create the necessary directory structure
2. Provide clear instructions on where to place your CSV file
3. Fall back to synthetic data for demonstration

## Testing

```bash
# Syntax check passed
python3 -m py_compile examples/naive_forecast_example.py

# Example runs successfully (falls back to synthetic data without POS API)
python3 examples/naive_forecast_example.py
```

## Files Modified

- `examples/naive_forecast_example.py` - Added auto-download functionality
- `NAIVE_MODEL_IMPLEMENTATION.md` - Updated documentation

## Lines of Code

- Added: ~80 lines of ETL integration logic
- Modified: Prerequisites and documentation
- Total file size: ~250 lines (up from ~194)
