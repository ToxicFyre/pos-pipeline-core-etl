"""CLI wrapper for the payments forecasting pipeline.

This module provides a command-line interface for running forecasts.
All core forecasting logic is in pos_core.forecasting.api.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from pos_core.forecasting.api import ForecastConfig, run_payments_forecast
from pos_core.forecasting.data.loaders import load_payments_data
from pos_core.forecasting.formatters.console import (
    format_forecast_for_console,
)
from pos_core.forecasting.formatters.telegram import format_telegram_message

# Optional Telegram support (best-effort, not required)
try:
    from utils.telegram_notifier import get_credentials, send_message

    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False


def main() -> None:
    """Main CLI entry point for the forecasting pipeline.

    Parses command-line arguments, loads data, runs forecasts, and optionally
    sends results to Telegram.
    """
    parser = argparse.ArgumentParser(description="Run payments forecast.")
    parser.add_argument(
        "--file",
        type=str,
        help="Path to aggregated_payments_daily.csv. If not provided, uses default from config.",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=7,
        help="Number of days to forecast ahead (default: 7)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="arima",
        choices=["arima", "naive"],
        help="Forecast model to use (default: arima). Options: arima, naive",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Payments Forecasting Pipeline")
    print("=" * 60)

    # Get Telegram credentials (optional)
    bot_token = None
    chat_id = None
    if TELEGRAM_AVAILABLE:
        bot_token, chat_id = get_credentials()
        if not bot_token or not chat_id:
            print("Warning: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
            print("Forecasts will be generated but not sent to Telegram")

    try:
        # Load data
        csv_path = Path(args.file) if args.file else None
        print("\n[1/3] Loading payments data...")
        if csv_path:
            print(f"  Reading from: {csv_path}")
            if not csv_path.exists():
                raise FileNotFoundError(f"Payments data file not found: {csv_path}")
            payments_df = pd.read_csv(csv_path)
        else:
            print("  Using default path from config")
            payments_df = load_payments_data(csv_path)

        payments_df["fecha"] = pd.to_datetime(payments_df["fecha"])
        print(f"[OK] Loaded {len(payments_df)} rows")

        # Configure and run forecast
        print(f"\n[2/3] Generating {args.horizon}-day forecasts using {args.model} model...")
        config = ForecastConfig(horizon_days=args.horizon, model_type=args.model)
        result = run_payments_forecast(payments_df, config=config)
        branches_list = result.metadata.get("branches", [])
        branches_count = len(branches_list) if isinstance(branches_list, list) else 0
        print(f"[OK] Generated forecasts for {branches_count} branches")

        # Format and display results
        print("\n[3/3] Formatting results...")

        # Format for console
        console_message = format_forecast_for_console(result)
        print("\n" + "=" * 60)
        print("Forecast Results:")
        print("=" * 60)
        print(console_message)
        print("=" * 60)

        # Format for Telegram (HTML)
        telegram_message = format_telegram_message(result)
        print(f"\nTelegram message length: {len(telegram_message)} characters")

        # Send to Telegram if credentials are available
        if TELEGRAM_AVAILABLE and bot_token and chat_id:
            print("\nSending to Telegram...")
            success = send_message(
                telegram_message,
                bot_token=bot_token,
                chat_id=chat_id,
                verbose=args.verbose,
            )
            if success:
                print("[OK] Message sent to Telegram")
            else:
                print("[ERROR] Failed to send message to Telegram")
        elif not TELEGRAM_AVAILABLE:
            print("[INFO] Telegram support not available (utils.telegram_notifier not found)")

        print("\n[OK] Pipeline completed successfully")

    except Exception as e:
        error_msg = f"Pipeline failed: {e}"
        print(f"\n[ERROR] {error_msg}")

        # Try to send error notification to Telegram (best-effort)
        if TELEGRAM_AVAILABLE and bot_token and chat_id:
            try:
                error_telegram = f"[ERROR] <b>Forecast Pipeline Error</b>\n\n{error_msg}"
                send_message(error_telegram, bot_token=bot_token, chat_id=chat_id)
            except Exception as send_error:
                print(f"[WARNING] Failed to send error notification: {send_error}")

        raise


if __name__ == "__main__":
    main()
