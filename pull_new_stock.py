import os
import pandas as pd
import yfinance as yf
import argparse
from datetime import datetime, timedelta

# CONFIGURATION
RAW_DIR = "Raw"
SPY_PATH = os.path.join(RAW_DIR, "Reference", "SPY.csv")
INTERVAL = "5m"

def main():
    parser = argparse.ArgumentParser(description="Download all available 5m data for a new stock synced to SPY.")
    parser.add_argument("symbol", type=str.upper, help="Stock ticker symbol (e.g., NVDA)")
    parser.add_argument("subfolder", type=str, nargs='?', default="Misc", 
                        help="Subdirectory under Raw to save (default: Misc)")
    args = parser.parse_args()

    symbol = args.symbol
    target_dir = os.path.join(RAW_DIR, args.subfolder)
    os.makedirs(target_dir, exist_ok=True)
    file_path = os.path.join(target_dir, f"{symbol}.csv")

    if not os.path.exists(SPY_PATH):
        print(f"Error: Master clock {SPY_PATH} not found.")
        return

    # 1. Load Master Clock (SPY)
    spy = pd.read_csv(SPY_PATH, index_col='timestamp', parse_dates=True)
    spy_start = spy.index.min()
    spy_end = spy.index.max()

    # 2. Calculate a "Safe" Start Date
    # Yahoo 5m data limit is ~60 days. If SPY start is older, we clip it.
    sixty_days_ago = datetime.now() - timedelta(days=59)
    fetch_start = max(spy_start, sixty_days_ago)

    print(f"Syncing {symbol} to SPY master clock...")
    print(f"Master Range:  {spy_start} to {spy_end}")
    if fetch_start > spy_start:
        print(f"Yahoo Limit:   Adjusting fetch start to {fetch_start.date()} (60-day window)")

    # 3. Fetch data
    try:
        df_new = yf.download(
            tickers=symbol,
            start=fetch_start.strftime('%Y-%m-%d'),
            interval=INTERVAL,
            progress=False,
            auto_adjust=True
        )

        if df_new.empty:
            print(f"No data found for {symbol} within the last 60 days.")
            return

        # Clean yfinance format
        if isinstance(df_new.columns, pd.MultiIndex):
            df_new.columns = df_new.columns.get_level_values(0)
        df_new.index = df_new.index.tz_localize(None)
        df_new = df_new[['Open', 'High', 'Low', 'Close', 'Volume']]

        # 4. Sync to SPY (Only keep rows that exist in SPY)
        df_synced = df_new[df_new.index.isin(spy.index)].copy()

        # 5. Precision & Cleanup
        df_synced[['Open', 'High', 'Low', 'Close']] = df_synced[['Open', 'High', 'Low', 'Close']].round(4)
        df_synced['Volume'] = df_synced['Volume'].fillna(0).astype(int)
        df_synced.index.name = 'timestamp'

        # 6. Save and Detailed Report
        df_synced.sort_index().to_csv(file_path)
        
        actual_start = df_synced.index.min()
        actual_end = df_synced.index.max()
        
        print("-" * 40)
        print(f"SUCCESS: {symbol}")
        print(f"Destination: {file_path}")
        print(f"Rows Saved:  {len(df_synced)}")
        print(f"First TS:    {actual_start}")
        print(f"Last TS:     {actual_end}")
        print("-" * 40)

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")

if __name__ == "__main__":
    main()
