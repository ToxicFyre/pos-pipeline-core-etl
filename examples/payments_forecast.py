"""Example: Forecasting Workflow using new query API

This example demonstrates how to generate forecasts for the next 7 days using
the new query API. The API automatically handles getting historical data and
running the forecast.

Prerequisites:
- Set WS_BASE environment variable (if downloading data)
- Create utils/sucursales.json with branch configuration
- Ensure data/ directory structure exists (or modify paths below)
"""

from pathlib import Path

from pos_core.etl import PaymentsETLConfig, get_payments_forecast

# Set up configuration
data_root = Path("data")
sucursales_json = Path("utils/sucursales.json")

config = PaymentsETLConfig.from_root(data_root, sucursales_json)

# Get payments forecast using the query API
# This automatically:
# 1. Gets historical payments data (last 3 years)
# 2. Runs the forecast
# 3. Returns the forecast DataFrame
print("Getting payments forecast...")
forecast_df = get_payments_forecast(
    as_of="2025-11-24",  # Forecast as of this date - MODIFY AS NEEDED
    horizon_weeks=13,  # Forecast 13 weeks ahead - MODIFY AS NEEDED
    config=config,
    refresh=False,  # Use existing data if available
)

print("\nForecast DataFrame:")
print(forecast_df.head(20))

print("\nThe forecast DataFrame has columns:")
print("- sucursal: Branch name")
print("- fecha: Forecast date")
print("- metric: Metric name (ingreso_efectivo, ingreso_credito, etc.)")
print("- valor: Forecasted value")

# Example: Get forecast for a specific branch and metric
if len(forecast_df) > 0:
    sample_branch = forecast_df["sucursal"].iloc[0]
    sample_metric = forecast_df["metric"].iloc[0]
    branch_metric_forecast = forecast_df[
        (forecast_df["sucursal"] == sample_branch)
        & (forecast_df["metric"] == sample_metric)
    ]
    print(f"\n{sample_branch} {sample_metric} forecast (next 13 weeks):")
    print(branch_metric_forecast[["fecha", "valor"]])

# Example: Pivot forecast for easier viewing
forecast_pivot = forecast_df.pivot_table(
    index=["sucursal", "fecha"], columns="metric", values="valor"
)
print("\nForecast Pivot (first 10 rows):")
print(forecast_pivot.head(10))

# Save results
forecast_output = data_root / "c_processed" / "forecasts" / "next_13_weeks_forecast.csv"
forecast_output.parent.mkdir(parents=True, exist_ok=True)
forecast_df.to_csv(forecast_output, index=False)
print(f"\nSaved forecast to: {forecast_output}")

# Note: For deposit schedule and detailed metadata, use run_payments_forecast directly
# if you need those additional outputs
