import os
import glob
import pandas as pd
import yfinance as yf

# CONFIGURATION
RAW_DIR = "Raw"
SPY_PATH = os.path.join(RAW_DIR, "Reference", "SPY.csv")
INTERVAL = "5m"

def sync_all_to_spy():
    if not os.path.exists(SPY_PATH):
        print(f"Error: Master clock {SPY_PATH} not found. Run catch_up_spy.py first.")
        return

    # 1. Load Master Clock (SPY)
    spy = pd.read_csv(SPY_PATH, index_col='timestamp', parse_dates=True)
    spy_index = spy.index
    spy_last_ts = spy_index.max()
    
    # Track results for final summary
    summary = []

    # 2. Find all ticker files (recursively, excluding SPY)
    ticker_files = [f for f in glob.glob(os.path.join(RAW_DIR, "**", "*.csv"), recursive=True) 
                    if "SPY.csv" not in f]

    print(f"Starting sync for {len(ticker_files)} tickers against SPY (Last TS: {spy_last_ts})")
    print("-" * 60)

    for file_path in ticker_files:
        ticker = os.path.basename(file_path).replace(".csv", "")
        status = "Synced"
        
        try:
            df = pd.read_csv(file_path, index_col='timestamp', parse_dates=True)
            
            # RULE 1: Remove any local data that doesn't exist in the SPY master clock
            # This handles orphans and ensures time-alignment.
            df = df[df.index.isin(spy_index)]

            # RULE 2: Check for a gap at the end (the "tail")
            stock_last_ts = df.index.max()
            
            if pd.isna(stock_last_ts) or stock_last_ts < spy_last_ts:
                # If file is empty, we can't know when it "started", 
                # so we default to SPY's start to be safe (or a specific date).
                fetch_start = stock_last_ts.strftime('%Y-%m-%d') if not pd.isna(stock_last_ts) else "2026-01-01"
                
                new_data = yf.download(
                    tickers=ticker,
                    start=fetch_start,
                    interval=INTERVAL,
                    progress=False,
                    auto_adjust=True
                )

                if not new_data.empty:
                    # Clean yfinance format
                    if isinstance(new_data.columns, pd.MultiIndex):
                        new_data.columns = new_data.columns.get_level_values(0)
                    new_data.index = new_data.index.tz_localize(None)
                    new_data = new_data[['Open', 'High', 'Low', 'Close', 'Volume']]
                    
                    # Merge: overwrite existing partial data with fresh data
                    df = pd.concat([df, new_data])
                    df = df[~df.index.duplicated(keep='last')]
                    
                    # Re-filter after merge to ensure new data respects SPY's clock
                    df = df[df.index.isin(spy_index)]
                    status = "Updated"
                else:
                    status = "No new data (Check Yahoo 60-day limit)"

            # Final save
            df.sort_index().to_csv(file_path)
            summary.append(f"{ticker:10} | {status:20} | Rows: {len(df)}")
            print(f"Processed {ticker}: {status}")

        except Exception as e:
            summary.append(f"{ticker:10} | ERROR: {str(e)[:30]}")
            print(f"Error processing {ticker}: {e}")

    # Final Summary Echo
    print("\n" + "="*30)
    print("SYNC SUMMARY REPORT")
    print("="*30)
    for line in summary:
        print(line)
    print("="*30)

if __name__ == "__main__":
    sync_all_to_spy()
