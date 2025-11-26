"""Example 3: Forecasting Workflow

This example demonstrates how to generate forecasts for the next 7 days using
historical payment data. At the end, you'll have forecasted values for each
branch and metric, plus a deposit schedule for cash flow planning.

Prerequisites:
- Run payments_full_etl.py first (or have aggregated_payments_daily.csv available)
- Ensure data/c_processed/payments/aggregated_payments_daily.csv exists
- Modify paths below if your data is in a different location
"""

from pathlib import Path
import pandas as pd

from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Load the aggregated payments dataset (from Example 2 or existing file)
# MODIFY PATH AS NEEDED
data_root = Path("data")
payments_file = data_root / "c_processed" / "payments" / "aggregated_payments_daily.csv"

if not payments_file.exists():
    raise FileNotFoundError(
        f"Payments file not found: {payments_file}\n"
        "Please run payments_full_etl.py first or ensure the file exists."
    )

payments_df = pd.read_csv(payments_file)
payments_df['fecha'] = pd.to_datetime(payments_df['fecha'])

# Configure forecast - MODIFY AS NEEDED
forecast_config = ForecastConfig(
    horizon_days=7,  # Forecast next 7 days
    metrics=[
        "ingreso_efectivo",
        "ingreso_credito",
        "ingreso_debito",
        "ingreso_total"
    ],
    branches=None  # Forecast for all branches (or specify: ["Kavia", "QIN"])
)

# Run forecast
print("Running forecast...")
result = run_payments_forecast(payments_df, config=forecast_config)

# Access forecast results
print("\nForecast DataFrame:")
print(result.forecast.head(20))

print("\nThe forecast DataFrame has columns:")
print("- sucursal: Branch name")
print("- fecha: Forecast date")
print("- metric: Metric name (ingreso_efectivo, ingreso_credito, etc.)")
print("- valor: Forecasted value")

# Access deposit schedule (cash flow view)
print("\nDeposit Schedule:")
print(result.deposit_schedule)

print("\nThe deposit schedule has columns:")
print("- fecha: Deposit date")
print("- efectivo: Total cash deposits")
print("- credito: Total credit card deposits")
print("- debito: Total debit card deposits")
print("- total: Total deposits")

# Access metadata
print(f"\nForecast Metadata:")
print(f"Branches: {result.metadata['branches']}")
print(f"Metrics: {result.metadata['metrics']}")
print(f"Horizon: {result.metadata['horizon_days']} days")
print(f"Last historical date: {result.metadata['last_historical_date']}")
print(f"Successful forecasts: {result.metadata['successful_forecasts']}")
print(f"Failed forecasts: {result.metadata['failed_forecasts']}")

# Example: Get forecast for a specific branch and metric
if len(result.forecast) > 0:
    sample_branch = result.forecast['sucursal'].iloc[0]
    sample_metric = result.forecast['metric'].iloc[0]
    branch_metric_forecast = result.forecast[
        (result.forecast['sucursal'] == sample_branch) &
        (result.forecast['metric'] == sample_metric)
    ]
    print(f"\n{sample_branch} {sample_metric} forecast (next 7 days):")
    print(branch_metric_forecast[['fecha', 'valor']])

# Example: Pivot forecast for easier viewing
forecast_pivot = result.forecast.pivot_table(
    index=['sucursal', 'fecha'],
    columns='metric',
    values='valor'
)
print(f"\nForecast Pivot (first 10 rows):")
print(forecast_pivot.head(10))

# Save results
forecast_output = data_root / "c_processed" / "forecasts" / "next_7_days_forecast.csv"
forecast_output.parent.mkdir(parents=True, exist_ok=True)
result.forecast.to_csv(forecast_output, index=False)
print(f"\nSaved forecast to: {forecast_output}")

deposit_output = data_root / "c_processed" / "forecasts" / "next_7_days_deposits.csv"
result.deposit_schedule.to_csv(deposit_output, index=False)
print(f"Saved deposit schedule to: {deposit_output}")

