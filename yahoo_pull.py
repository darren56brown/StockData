import yfinance as yf
import pandas as pd
import os
import argparse
from datetime import datetime, timedelta

# --- CONFIGURATION ---
DATA_DIR = os.path.expanduser("./Staging")
os.makedirs(DATA_DIR, exist_ok=True)
# ---------------------

INTRADAY_INTERVALS = {'1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h'}

def is_intraday(interval):
    return interval in INTRADAY_INTERVALS

def main():
    parser = argparse.ArgumentParser(
        description="Download OHLCV data from Yahoo Finance for a stock — month/year for daily+, or specific day(s) for intraday.",
        epilog="""Examples:
  python pull_yahoo.py TSLA 2025-03                     # daily data for whole March 2025
  python pull_yahoo.py NVDA 2026-03-04 --interval 5m    # 5-min data for March 4, 2026 only
  python pull_yahoo.py AAPL 2026-03-01 --interval 1h    # hourly for a recent date

Notes:
- For intraday intervals (<1d): must use YYYY-MM-DD (single day recommended; max ~7–60 days back depending on interval).
- For daily+ intervals: use YYYY-MM (fetches full month).
- Intraday history limited: 1m ≈7 days back, others ≈60 days back.""",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "symbol",
        type=str.upper,
        help="Stock ticker symbol (e.g., TSLA, NVDA, AAPL)"
    )
    
    parser.add_argument(
        "date_input",
        type=str,
        help="For daily+: YYYY-MM (whole month). For intraday: YYYY-MM-DD (specific day)"
    )
    
    parser.add_argument(
        "--interval",
        type=str.lower,
        default="1d",
        help="Data interval: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo (default: 1d). "
             "Intraday (<1d) limited to recent data (~7–60 days back)."
    )

    args = parser.parse_args()

    symbol = args.symbol
    date_input = args.date_input
    interval = args.interval

    try:
        if is_intraday(interval):
            # Require full date for intraday
            dt = datetime.strptime(date_input, '%Y-%m-%d')
            start_date = dt
            end_date = dt + timedelta(days=1)  # Fetch one full day
            print(f"Intraday mode: Fetching {interval} data for single day {dt.date()}")
        else:
            # Monthly for daily+
            year, month = map(int, date_input.split('-'))
            if not (1 <= month <= 12):
                raise ValueError("Month must be 01–12")
            start_date = datetime(year, month, 1)
            next_month = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
            end_date = next_month - timedelta(days=1)
            print(f"Daily+ mode: Fetching {interval} data for whole month {year}-{month:02d}")

    except Exception as e:
        print(f"Error: Invalid date format '{date_input}' for interval '{interval}' ({e})")
        if is_intraday(interval):
            print("For intraday intervals, use YYYY-MM-DD (e.g., 2026-03-04)")
        else:
            print("For daily+ intervals, use YYYY-MM (e.g., 2025-03)")
        parser.print_help()
        return

    print(f"Fetching data for {symbol} from {start_date.date()} to {end_date.date()}...")

    try:
        df = yf.download(
            tickers=symbol,
            start=start_date,
            end=end_date + timedelta(days=1),
            interval=interval,
            progress=False,
            auto_adjust=True,
            actions=False
        )

        if df.empty:
            print(f"No data returned for {symbol} in the requested range at {interval} interval.")
            print("Possible reasons: future/old date, invalid symbol, or intraday range too far back.")
            return

        # Clean columns - handle potential MultiIndex from yfinance
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Keep only what we want
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']]

        # Remove timezone offset (clean timestamps)
        df.index = df.index.tz_localize(None)

        # Set clean index name
        df.index.name = 'timestamp'

        # Filename logic...
        date_str = date_input if is_intraday(interval) else date_input
        filename = f"{symbol}_{date_str}_{interval}.csv"
        filepath = os.path.join(DATA_DIR, filename)

        df.to_csv(filepath)
        print(f"Success! Saved {len(df)} rows to {filepath}")
        print(f"First few lines of CSV preview:\n{df.head(2).to_csv()}")

    except Exception as e:
        print(f"Error fetching data: {e}")

if __name__ == "__main__":
    main()

    