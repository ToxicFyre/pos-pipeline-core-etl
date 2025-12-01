"""Example: Forecasting Workflow using new API

This example demonstrates how to generate forecasts for the next 7 days using
the new domain-oriented API. The API automatically handles getting historical
data and running the forecast.

Prerequisites:
- Set WS_BASE environment variable (if downloading data)
- Create utils/sucursales.json with branch configuration
- Ensure data/ directory structure exists (or modify paths below)
"""

from pathlib import Path

from pos_core import DataPaths
from pos_core.forecasting import ForecastConfig, run_payments_forecast
from pos_core.payments import marts as payments_marts

# Set up configuration with new unified DataPaths
data_root = Path("data")
sucursales_json = Path("utils/sucursales.json")

paths = DataPaths.from_root(data_root, sucursales_json)

# Get historical payment data (daily mart)
print("Getting historical payment data...")
payments_df = payments_marts.fetch_daily(
    paths=paths,
    start_date="2022-01-01",
    end_date="2025-11-24",  # MODIFY AS NEEDED
    mode="missing",  # Use existing data if available
)

print(f"Loaded {len(payments_df)} rows of historical data")

# Configure and run forecast
config = ForecastConfig(horizon_days=91)  # 13 weeks

print("Running payments forecast...")
result = run_payments_forecast(payments_df, config)

print("\nForecast DataFrame:")
print(result.forecast.head(20))

print("\nThe forecast DataFrame has columns:")
print("- sucursal: Branch name")
print("- fecha: Forecast date")
print("- metric: Metric name (ingreso_efectivo, ingreso_credito, etc.)")
print("- valor: Forecasted value")

# Example: Get forecast for a specific branch and metric
if len(result.forecast) > 0:
    sample_branch = result.forecast["sucursal"].iloc[0]
    sample_metric = result.forecast["metric"].iloc[0]
    branch_metric_forecast = result.forecast[
        (result.forecast["sucursal"] == sample_branch)
        & (result.forecast["metric"] == sample_metric)
    ]
    print(f"\n{sample_branch} {sample_metric} forecast (next 13 weeks):")
    print(branch_metric_forecast[["fecha", "valor"]])

# Example: Pivot forecast for easier viewing
forecast_pivot = result.forecast.pivot_table(
    index=["sucursal", "fecha"], columns="metric", values="valor"
)
print("\nForecast Pivot (first 10 rows):")
print(forecast_pivot.head(10))

# Save results
forecast_output = data_root / "c_processed" / "forecasts" / "next_13_weeks_forecast.csv"
forecast_output.parent.mkdir(parents=True, exist_ok=True)
result.forecast.to_csv(forecast_output, index=False)
print(f"\nSaved forecast to: {forecast_output}")

# Access the deposit schedule
print("\nDeposit Schedule (first 10 rows):")
print(result.deposit_schedule.head(10))
