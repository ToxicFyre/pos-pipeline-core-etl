# Quickstart

Get started with POS Core ETL in minutes.

## Basic ETL Workflow

```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig, build_payments_dataset

# Configure
config = PaymentsETLConfig.from_data_root(Path("data"))

# Run ETL for a date range
payments = build_payments_dataset(
    start_date="2025-01-01",
    end_date="2025-01-31",
    config=config
)

print(f"Processed {len(payments)} rows")
```

## Forecasting

```python
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Generate 7-day forecast
forecast = run_payments_forecast(
    payments,
    ForecastConfig(horizon_days=7)
)

print(forecast.forecast.head())
```

## Quality Assurance

```python
from pos_core.qa import run_payments_qa

# Run QA checks
qa_result = run_payments_qa(payments)

print(f"Missing days: {qa_result.summary['missing_days_count']}")
print(f"Anomalies: {qa_result.summary['zscore_anomalies_count']}")
```

## See Also

- [Configuration](configuration.md) - Detailed configuration options
- [Examples](examples.md) - Complete runnable examples
- [API Reference](../api-reference/etl.md) - Full API documentation

