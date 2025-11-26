# Quickstart

Get started with POS Core ETL in minutes.

## Basic ETL Workflow

The recommended approach is to use the query functions, which automatically handle running ETL stages and provide automatic idempotence:

```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig, get_payments

# Configure
config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))

# Get payments data (automatically runs ETL stages only if needed)
payments = get_payments(
    start_date="2025-01-01",
    end_date="2025-01-31",
    config=config,
    refresh=False,  # Use existing data if available
)

print(f"Processed {len(payments)} rows")
```

**Alternative**: Use `build_payments_dataset()` for complete orchestration:

```python
from pos_core.etl import PaymentsETLConfig, build_payments_dataset

config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
payments = build_payments_dataset("2025-01-01", "2025-01-31", config)
```

## Sales Data

Get sales data aggregated at different levels:

```python
from pos_core.etl import SalesETLConfig, get_sales

config = SalesETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))

# Get sales by ticket
df_ticket = get_sales("2025-01-01", "2025-01-31", config, level="ticket")

# Get sales by product group (pivot table)
df_group = get_sales("2025-01-01", "2025-01-31", config, level="group")
```

## Forecasting

The easiest way to get forecasts is using the query API:

```python
from pos_core.etl import PaymentsETLConfig, get_payments_forecast

config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))

# Get 13-week forecast (automatically gets historical data)
forecast = get_payments_forecast(
    as_of="2025-11-24",
    horizon_weeks=13,
    config=config,
)

print(forecast.head())
```

**Alternative**: For full control including deposit schedule and metadata:

```python
from pos_core.etl import PaymentsETLConfig, get_payments
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Get historical data
payments = get_payments("2022-01-01", "2025-11-24", config)

# Run forecast with full result
result = run_payments_forecast(
    payments,
    ForecastConfig(horizon_days=91)  # 13 weeks
)

print(result.forecast.head())
print(result.deposit_schedule)
```

## Quality Assurance

```python
from pos_core.etl import PaymentsETLConfig, get_payments
from pos_core.qa import run_payments_qa

# Get payments data
config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
payments = get_payments("2025-01-01", "2025-01-31", config)

# Run QA checks
qa_result = run_payments_qa(payments)

print(f"Missing days: {qa_result.summary['missing_days_count']}")
print(f"Anomalies: {qa_result.summary['zscore_anomalies_count']}")
```

## See Also

- [Configuration](configuration.md) - Detailed configuration options
- [Examples](examples.md) - Complete runnable examples
- [API Reference](../api-reference/etl.md) - Full API documentation
