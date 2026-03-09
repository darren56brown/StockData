import os
import pandas as pd
import yfinance as yf
from datetime import datetime

# CONFIGURATION
SPY_PATH = os.path.join("Raw", "Reference", "SPY.csv")
INTERVAL = "5m"

def catch_up_spy():
    if not os.path.exists(SPY_PATH):
        print(f"Error: {SPY_PATH} not found.")
        return

    # 1. Load existing SPY data
    df_existing = pd.read_csv(SPY_PATH)
    df_existing['timestamp'] = pd.to_datetime(df_existing['timestamp'])
    df_existing.set_index('timestamp', inplace=True)
    
    # 2. Identify the last date and "Rewind"
    # We drop everything from the last recorded date to ensure a full refresh of that final day.
    last_timestamp = df_existing.index.max()
    last_date_str = last_timestamp.strftime('%Y-%m-%d')
    
    print(f"Last recorded entry: {last_timestamp}")
    print(f"Refreshing all data starting from: {last_date_str}")
    
    # Remove the partial/final day from existing data
    df_clean = df_existing[df_existing.index.date < last_timestamp.date()].copy()

    # 3. Fetch fresh data from Yahoo Finance
    # yfinance 'end' is exclusive, so we use tomorrow's date to include today
    today_plus_one = (datetime.now() + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    
    try:
        df_new = yf.download(
            tickers="SPY",
            start=last_date_str,
            end=today_plus_one,
            interval=INTERVAL,
            progress=False,
            auto_adjust=True
        )

        if df_new.empty:
            print("No new data found or markets are currently closed.")
            return

        # Handle MultiIndex if present and clean timezone
        if isinstance(df_new.columns, pd.MultiIndex):
            df_new.columns = df_new.columns.get_level_values(0)
        df_new.index = df_new.index.tz_localize(None)
        df_new.index.name = 'timestamp'
        df_new = df_new[['Open', 'High', 'Low', 'Close', 'Volume']]

        # 4. Merge and Save
        # Combine the old 'clean' data with the new 'full' day(s)
        df_final = pd.concat([df_clean, df_new])
        
        # Final deduplication just in case
        df_final = df_final[~df_final.index.duplicated(keep='last')].sort_index()
        
        df_final.to_csv(SPY_PATH)
        print(f"Success! {SPY_PATH} updated. New last entry: {df_final.index.max()}")

    except Exception as e:
        print(f"Error fetching data: {e}")

if __name__ == "__main__":
    catch_up_spy()
