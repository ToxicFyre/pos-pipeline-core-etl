"""Daily payments forecasting pipeline with model-agnostic architecture.

This module orchestrates:
1. Downloading latest payments data
2. Training forecasting models for 4 metrics across 7 branches
3. Generating 7-day forecasts
4. Sending formatted Telegram messages via utils.telegram_notifier

The pipeline is designed to support multiple model types (ARIMA, Prophet, LSTM, etc.)
through a unified model interface.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

# Add src to path for imports
src_dir = Path(__file__).resolve().parent.parent
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

# Add project root to path for utils imports
project_root = src_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pos_etl.build_payments_dataset import build_payments_dataset
from pos_forecasting.config import BRANCHES, FORECAST_DAYS, METRICS
from pos_forecasting.data.loaders import load_payments_data
from pos_forecasting.data.preparation import (
    build_daily_series,
    calculate_ingreso_total,
)
from pos_forecasting.formatters.console import sanitize_for_console
from pos_forecasting.formatters.telegram import format_telegram_message
from pos_forecasting.models.arima import LogARIMAModel
from pos_forecasting.models.base import ForecastModel
from utils.telegram_notifier import get_credentials, send_message


def get_model(model_type: str = "arima") -> ForecastModel:
    """Factory function to get a forecasting model instance.
    
    Args:
        model_type: Type of model to use ("arima", "prophet", "lstm", etc.)
        
    Returns:
        ForecastModel instance
        
    Raises:
        ValueError: If model_type is not supported
    """
    if model_type == "arima":
        return LogARIMAModel()
    else:
        raise ValueError(f"Unsupported model type: {model_type}. Supported types: 'arima'")


def generate_forecasts(
    csv_path: Optional[Path] = None,
    model_type: str = "arima",
) -> Tuple[Dict[str, Dict[str, pd.Series]], pd.DataFrame, date]:
    """Generate 7-day forecasts for all branches and metrics.
    
    Args:
        csv_path: Path to aggregated_payments_daily.csv. If None, uses default from config.
        model_type: Type of model to use ("arima", "prophet", etc.). Default: "arima"
        
    Returns:
        Tuple of:
        - Nested dictionary: {branch: {metric: forecast_series}}
        - DataFrame with historical data
        - Last date in historical data
        
    Raises:
        FileNotFoundError: If payments data file is not found
        ValueError: If no forecasts are generated
    """
    # Load data
    df = load_payments_data(csv_path)
    
    # Calculate total
    df = calculate_ingreso_total(df)
    
    # Get last date in historical data
    last_historical_date = df["fecha"].max().date()
    
    # Get model instance
    model = get_model(model_type)
    
    forecasts: Dict[str, Dict[str, pd.Series]] = {}
    
    successful_forecasts = 0
    failed_forecasts = 0
    
    for branch in BRANCHES:
        forecasts[branch] = {}
        
        for metric in METRICS:
            try:
                # Build series (missing days are already filled with 0.0)
                series = build_daily_series(df, branch, metric)
                
                # Series should have no NaN values (missing days are 0.0)
                # But check for any remaining NaN/inf just in case
                series = series.replace([np.inf, -np.inf], np.nan).fillna(0.0)
                
                if len(series) < 30:
                    print(f"  Warning: {branch} - {metric}: insufficient data ({len(series)} obs), skipping")
                    failed_forecasts += 1
                    continue
                
                # Train model and forecast
                print(f"  Training {branch} - {metric}...")
                trained_model = model.train(series)
                # Pass last_date for proper index creation
                last_date = series.index[-1]
                forecast = model.forecast(trained_model, steps=FORECAST_DAYS, last_date=last_date)
                forecasts[branch][metric] = forecast
                successful_forecasts += 1
                
            except Exception as e:
                print(f"  Error forecasting {branch} - {metric}: {e}")
                failed_forecasts += 1
                continue
    
    print(f"\n  Forecast summary: {successful_forecasts} successful, {failed_forecasts} failed")
    
    # Check if we have any forecasts at all
    total_forecasts = sum(len(metrics) for metrics in forecasts.values())
    if total_forecasts == 0:
        raise ValueError("No forecasts were generated. Check data availability and model training errors.")
    
    return forecasts, df, last_historical_date


def main() -> None:
    """Main entry point for the forecasting pipeline.

    Orchestrates the complete forecasting workflow:
    1. Downloads latest payments data from POS (3 years back to yesterday)
    2. Generates 7-day forecasts for all branches and metrics using forecasting models
    3. Formats forecasts as Telegram message with HTML formatting
    4. Sends message to Telegram (if credentials are configured)

    The pipeline handles errors gracefully and attempts to send error notifications
    to Telegram if the main pipeline fails.

    Environment Variables:
        TELEGRAM_BOT_TOKEN: Telegram bot token for notifications (optional)
        TELEGRAM_CHAT_ID: Telegram chat ID for notifications (optional)

    Raises:
        FileNotFoundError: If payments data file is not found.
        ValueError: If no forecasts are generated or message is empty.
        Exception: Any other error during pipeline execution (also sent to Telegram).
    """
    print("=" * 60)
    print("Payments Forecasting Pipeline")
    print("=" * 60)
    
    # Get Telegram credentials from environment
    bot_token, chat_id = get_credentials()
    
    if not bot_token or not chat_id:
        print("Warning: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
        print("Forecasts will be generated but not sent to Telegram")
    
    try:
        # Step 1: Download latest data
        print("\n[1/3] Downloading latest payments data...")
        today = date.today()
        yesterday = today - timedelta(days=1)
        # Download from a reasonable start date (e.g., 3 years ago) to yesterday
        # (today's data is usually incomplete)
        start_date = date(today.year - 3, 1, 1)
        # Check for verbose flag from command line
        verbose = "--verbose" in sys.argv or "-v" in sys.argv
        
        build_payments_dataset(
            global_start=start_date,
            global_end=yesterday,
            max_days_per_chunk=180,
            dry_run=False,
            verbose=verbose,
        )
        print("[OK] Data download complete")
        
        # Step 2: Generate forecasts
        print("\n[2/3] Generating forecasts...")
        forecasts, historical_df, last_historical_date = generate_forecasts()
        print(f"[OK] Generated forecasts for {len(forecasts)} branches")
        print(f"  Last historical date: {last_historical_date}")
        
        # Step 3: Format and send message
        print("\n[3/3] Formatting and sending Telegram message...")
        
        # Validate we have forecasts
        total_forecasts = sum(len(metrics) for metrics in forecasts.values())
        if total_forecasts == 0:
            raise ValueError("No forecasts generated. Cannot send message.")
        
        message = format_telegram_message(forecasts, historical_df, last_historical_date)
        
        # Validate message is not empty
        if not message or len(message.strip()) == 0:
            raise ValueError("Formatted message is empty")
        
        # Print to console as well (sanitize to avoid encoding errors on Windows)
        print("\n" + "=" * 60)
        print("Forecast Message:")
        print("=" * 60)
        console_message = sanitize_for_console(message)
        print(console_message)
        print("=" * 60)
        print(f"Message length: {len(message)} characters")
        
        # Send to Telegram if credentials are available
        if bot_token and chat_id:
            success = send_message(message, bot_token=bot_token, chat_id=chat_id, verbose=verbose)
            if success:
                print("[OK] Message sent to Telegram")
            else:
                print("[ERROR] Failed to send message to Telegram")
        else:
            print("[WARNING] Skipping Telegram send (credentials not set)")
        
        print("\n[OK] Pipeline completed successfully")
        
    except Exception as e:
        error_msg = f"Pipeline failed: {e}"
        print(f"\n[ERROR] {error_msg}")
        
        # Try to send error notification to Telegram
        if bot_token and chat_id:
            error_telegram = f"[ERROR] <b>Forecast Pipeline Error</b>\n\n{error_msg}"
            send_message(error_telegram, bot_token=bot_token, chat_id=chat_id)
        
        raise


if __name__ == "__main__":
    main()

