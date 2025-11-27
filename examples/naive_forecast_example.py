"""Example: Using the Naive Last Week Forecasting Model

This example demonstrates how to use the NaiveLastWeekModel for forecasting.
The naive model predicts future values by finding equivalent historical weekdays
from previous weeks, while skipping holidays and their adjacent days.

This provides a simple baseline forecast alternative to ARIMA models.

Prerequisites:
- Have aggregated payments data (output from ETL pipeline)
- Data should include 'is_national_holiday' column if available
"""

from pathlib import Path
import pandas as pd

from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Example 1: Load existing payments data and run naive forecast
# Modify this path to point to your aggregated payments data
data_file = Path("data/c_processed/payments/aggregated_payments_daily.csv")

print("=" * 80)
print("Example 1: Naive Forecasting with Historical Data")
print("=" * 80)

if data_file.exists():
    # Load historical payments data
    print(f"\nLoading data from: {data_file}")
    payments_df = pd.read_csv(data_file)
    payments_df["fecha"] = pd.to_datetime(payments_df["fecha"])
    
    print(f"Loaded {len(payments_df)} rows of historical data")
    print(f"Date range: {payments_df['fecha'].min()} to {payments_df['fecha'].max()}")
    print(f"Branches: {payments_df['sucursal'].nunique()}")
    
    # Create config with naive model
    config = ForecastConfig(
        horizon_days=7,  # Forecast 7 days ahead
        model_type="naive",  # Use naive model instead of ARIMA
        branches=None,  # Forecast all branches (or specify list)
    )
    
    # Run forecast
    print("\nRunning naive forecast...")
    result = run_payments_forecast(payments_df, config=config)
    
    # Display results
    print("\n" + "=" * 80)
    print("Forecast Results:")
    print("=" * 80)
    print(f"\nSuccessful forecasts: {result.metadata['successful_forecasts']}")
    print(f"Failed forecasts: {result.metadata['failed_forecasts']}")
    
    print("\nForecast DataFrame (first 20 rows):")
    print(result.forecast.head(20))
    
    print("\nDeposit Schedule (cash flow view):")
    print(result.deposit_schedule)
    
    # Save results
    output_dir = Path("data/c_processed/forecasts")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    forecast_file = output_dir / "naive_forecast.csv"
    deposit_file = output_dir / "naive_deposit_schedule.csv"
    
    result.forecast.to_csv(forecast_file, index=False)
    result.deposit_schedule.to_csv(deposit_file, index=False)
    
    print(f"\nSaved forecast to: {forecast_file}")
    print(f"Saved deposit schedule to: {deposit_file}")

else:
    print(f"\nData file not found: {data_file}")
    print("Using synthetic data for demonstration instead...")
    print()

# Example 2: Comparison between ARIMA and Naive models
print("\n" + "=" * 80)
print("Example 2: Comparing ARIMA and Naive Models")
print("=" * 80)

# Create synthetic data for demonstration
num_days = 60
dates = pd.date_range("2025-01-01", periods=num_days, freq="D")

# Simulate weekly pattern with some noise
base_values = [100, 120, 130, 140, 150, 160, 180] * (num_days // 7 + 1)
base_values = base_values[:num_days]

synthetic_data = {
    "sucursal": ["Demo Branch"] * num_days,
    "fecha": dates,
    "ingreso_efectivo": [v + i * 2 for i, v in enumerate(base_values)],
    "ingreso_credito": [v * 2 + i * 3 for i, v in enumerate(base_values)],
    "ingreso_debito": [v * 1.5 + i * 1.5 for i, v in enumerate(base_values)],
    "is_national_holiday": [False] * num_days,
}
df_synthetic = pd.DataFrame(synthetic_data)

# Calculate ingreso_total
df_synthetic["ingreso_total"] = (
    df_synthetic["ingreso_efectivo"]
    + df_synthetic["ingreso_credito"]
    + df_synthetic["ingreso_debito"]
)

print(f"\nUsing synthetic data with {num_days} days of history")
print(f"Date range: {df_synthetic['fecha'].min()} to {df_synthetic['fecha'].max()}")

# Run ARIMA forecast
print("\n--- ARIMA Model ---")
config_arima = ForecastConfig(
    horizon_days=7,
    model_type="arima",
    branches=["Demo Branch"],
)
result_arima = run_payments_forecast(df_synthetic, config=config_arima)
print(f"Successful forecasts: {result_arima.metadata['successful_forecasts']}")

# Run Naive forecast
print("\n--- Naive Model ---")
config_naive = ForecastConfig(
    horizon_days=7,
    model_type="naive",
    branches=["Demo Branch"],
)
result_naive = run_payments_forecast(df_synthetic, config=config_naive)
print(f"Successful forecasts: {result_naive.metadata['successful_forecasts']}")

# Compare forecasts for ingreso_efectivo
print("\n" + "=" * 80)
print("Comparison: ingreso_efectivo forecasts")
print("=" * 80)

arima_efectivo = result_arima.forecast[
    result_arima.forecast["metric"] == "ingreso_efectivo"
][["fecha", "valor"]].rename(columns={"valor": "arima"})

naive_efectivo = result_naive.forecast[
    result_naive.forecast["metric"] == "ingreso_efectivo"
][["fecha", "valor"]].rename(columns={"valor": "naive"})

comparison = arima_efectivo.merge(naive_efectivo, on="fecha")
comparison["difference"] = comparison["arima"] - comparison["naive"]
comparison["pct_diff"] = (
    (comparison["difference"] / comparison["naive"]) * 100
).round(2)

print("\n", comparison.to_string(index=False))

print("\n" + "=" * 80)
print("Summary")
print("=" * 80)
print("""
The Naive Last Week Model:
- Uses historical data from equivalent weekdays in previous weeks
- Skips holidays and their adjacent days
- Provides a simple baseline forecast
- No statistical modeling or parameter tuning required
- Faster than ARIMA (no grid search)
- Best for stable patterns with clear weekly seasonality

When to use Naive vs ARIMA:
- Naive: Quick baseline, stable patterns, weekly seasonality
- ARIMA: Complex patterns, trends, multiple seasonality
""")
