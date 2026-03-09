import os
import subprocess
import pandas as pd
import argparse
from datetime import datetime, timedelta
from calendar import monthrange
import glob

# --- CONFIGURATION ---
DATA_DIR = os.path.expanduser("./Staging")
PULL_SCRIPT = "yahoo_pull.py"  # Name/location of your daily pull script
INTRADAY_INTERVALS = {'1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h'}
# ---------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch intraday data for each trading day in a (partial) month using yahoo_pull.py, "
                    "then combine all existing daily files for the full month into one monthly file.",
        epilog="""Example usage:
  python monthly_pull.py TSLA 2026-03 5m
  python monthly_pull.py NVDA 2025-12 1h --start-day 10 --end-day 20
  python monthly_pull.py AAPL 2026-02 15m --start-day 25
  python monthly_pull.py MSFT 2026-04 30m --end-day 15

Notes:
- --start-day defaults to 1
- --end-day defaults to the last day of the month
- --start-day and --end-day only affect which days to attempt fetching (skipping if exist).
- Always combines all existing daily files for the full month, regardless of --start-day and --end-day.
- Only intraday intervals are accepted.
- Skips days where the file already exists.
- Non-trading days produce empty/missing files — normal behavior."""
    )
    
    parser.add_argument("symbol", type=str.upper, help="Stock ticker symbol")
    parser.add_argument("year_month", type=str, help="Year and month in YYYY-MM format")
    parser.add_argument("interval", type=str.lower, help="Intraday interval: 1m,2m,5m,15m,30m,60m,90m,1h")

    parser.add_argument(
        "--start-day",
        type=int,
        default=1,
        help="Starting day of month (inclusive), default: 1"
    )
    parser.add_argument(
        "--end-day",
        type=int,
        default=None,
        help="Ending day of month (inclusive), default: last day of month"
    )

    args = parser.parse_args()

    symbol = args.symbol
    year_month = args.year_month
    interval = args.interval
    start_day = args.start_day
    end_day = args.end_day

    # Validate interval
    if interval not in INTRADAY_INTERVALS:
        print(f"Error: Interval '{interval}' not supported.")
        print("Supported:", ", ".join(sorted(INTRADAY_INTERVALS)))
        return

    # Parse year-month
    try:
        year, month = map(int, year_month.split('-'))
        if not (1 <= month <= 12):
            raise ValueError("Month must be 01–12")
    except Exception as e:
        print(f"Error: Invalid year-month '{year_month}' ({e})")
        return

    # Get number of days in this month
    _, last_day_of_month = monthrange(year, month)

    # Set default end_day if not provided
    if end_day is None:
        end_day = last_day_of_month

    # Validate day range
    if not (1 <= start_day <= last_day_of_month):
        print(f"Error: --start-day must be between 1 and {last_day_of_month}")
        return
    if not (1 <= end_day <= last_day_of_month):
        print(f"Error: --end-day must be between 1 and {last_day_of_month}")
        return
    if start_day > end_day:
        print("Error: --start-day cannot be after --end-day")
        return

    start_date = datetime(year, month, start_day)
    end_date   = datetime(year, month, end_day)

    print(f"Processing {symbol} for {year_month} at {interval} interval...")
    print(f"Date range for fetching: {start_date.date()} to {end_date.date()} "
          f"(days {start_day}–{end_day})\n")

    # Fetch loop (only for specified range)
    current_date = start_date
    fetched_count = 0

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        filename = f"{symbol}_{date_str}_{interval}.csv"
        filepath = os.path.join(DATA_DIR, filename)

        if os.path.exists(filepath):
            print(f"✓ {date_str} already exists → skipping fetch")
        else:
            print(f"→ Fetching {date_str} ...")
            cmd = [
                "python",
                PULL_SCRIPT,
                symbol,
                date_str,
                "--interval",
                interval
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"  Fetch failed for {date_str}:\n{result.stderr.strip()}")
            else:
                if os.path.exists(filepath) and os.path.getsize(filepath) > 100:
                    print(f"  Fetched successfully ({os.path.getsize(filepath)} bytes)")
                    fetched_count += 1
                else:
                    print(f"  Fetch ran but no/useless file for {date_str}")

        current_date += timedelta(days=1)

    print(f"\nFetch complete. Fetched {fetched_count} new days.")

    # Collect all existing daily files for the full month
    print("\nCombining all existing daily files for the month...")
    pattern = os.path.join(DATA_DIR, f"{symbol}_{year_month}-*_{interval}.csv")
    daily_files = sorted(glob.glob(pattern))

    # Combine
    combined_df = pd.DataFrame()
    valid_files = 0

    for filepath in daily_files:
        try:
            df_day = pd.read_csv(
                filepath,
                index_col='timestamp',
                parse_dates=True
            )
            if not df_day.empty:
                combined_df = pd.concat([combined_df, df_day])
                valid_files += 1
        except Exception as e:
            print(f"Warning: Could not read {os.path.basename(filepath)} → {e}")

    if combined_df.empty:
        print("No valid data found to combine.")
        return

    combined_df = combined_df.sort_index()

    monthly_filename = f"{symbol}_{year_month}_{interval}.csv"
    monthly_filepath = os.path.join(DATA_DIR, monthly_filename)

    combined_df.to_csv(monthly_filepath)
    print(f"\nSuccess!")
    print(f"Combined {len(combined_df)} rows from {valid_files} daily files")
    print(f"Saved to: {monthly_filepath}")

if __name__ == "__main__":
    main()