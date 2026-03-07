import os
import subprocess
import pandas as pd
import argparse
from datetime import datetime, timedelta

# --- CONFIGURATION ---
DATA_DIR = os.path.expanduser("./Staging")
PULL_SCRIPT = "yahoo_pull.py"  # Name/location of your daily pull script
INTRADAY_INTERVALS = {'1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h'}
# ---------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch intraday data for each trading day in a month using pull_yahoo.py, "
                    "then combine all daily files into one monthly file.",
        epilog="""Example usage:
  python monthly_pull.py TSLA 2026-03 5m
  python monthly_pull.py NVDA 2025-12 1h
  python monthly_pull.py AAPL 2026-02 15m

Note:
- Only intraday intervals are accepted (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h).
- Interval is required as the third argument.
- The script skips days where the file already exists.
- Non-trading days (weekends/holidays) will typically produce empty or missing files — that's normal.
- Combined file will be named like: SYMBOL_YYYY-MM_INTERVAL.csv"""
    )
    
    parser.add_argument(
        "symbol",
        type=str.upper,
        help="Stock ticker symbol (e.g., TSLA, NVDA, AAPL)"
    )
    
    parser.add_argument(
        "year_month",
        type=str,
        help="Year and month in YYYY-MM format (e.g., 2026-03)"
    )
    
    parser.add_argument(
        "interval",
        type=str.lower,
        help="Intraday interval: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h"
    )

    args = parser.parse_args()

    symbol = args.symbol
    year_month = args.year_month
    interval = args.interval

    # Validate interval
    if interval not in INTRADAY_INTERVALS:
        print(f"Error: Interval '{interval}' is not a supported intraday interval.")
        print("Supported: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h")
        return

    # Parse year-month → determine date range
    try:
        year, month = map(int, year_month.split('-'))
        if not (1 <= month <= 12):
            raise ValueError("Month must be 01–12")
        start_date = datetime(year, month, 1)
        next_month = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
        end_date = next_month - timedelta(days=1)
    except Exception as e:
        print(f"Error: Invalid year-month format '{year_month}' ({e})")
        print("Use YYYY-MM, e.g., 2026-03")
        return

    print(f"Processing {symbol} for {year_month} at {interval} interval...")
    print(f"Date range: {start_date.date()} to {end_date.date()}\n")

    # Collect daily files
    current_date = start_date
    daily_files = []
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
                # Check if file was actually created and has data
                if os.path.exists(filepath) and os.path.getsize(filepath) > 100:  # rough threshold
                    print(f"  Fetched successfully ({os.path.getsize(filepath)} bytes)")
                    fetched_count += 1
                else:
                    print(f"  Fetch ran but no/useless file created for {date_str} (likely no data)")

        # Add to list even if fetch failed (we'll skip empty/missing when combining)
        daily_files.append(filepath)
        current_date += timedelta(days=1)

    print(f"\nFetch complete. Attempted {fetched_count} new days.")

    # Combine existing daily files
    combined_df = pd.DataFrame()
    valid_files = 0

    for filepath in daily_files:
        if os.path.exists(filepath):
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
        print("No valid data found to combine. All days may be non-trading or fetches failed.")
        return

    # Sort just in case
    combined_df = combined_df.sort_index()

    # Save monthly combined file
    monthly_filename = f"{symbol}_{year_month}_{interval}.csv"
    monthly_filepath = os.path.join(DATA_DIR, monthly_filename)

    combined_df.to_csv(monthly_filepath)
    print(f"\nSuccess!")
    print(f"Combined {len(combined_df)} rows from {valid_files} daily files")
    print(f"Saved to: {monthly_filepath}")

if __name__ == "__main__":
    main()