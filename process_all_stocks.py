import os
import argparse
import pandas as pd
from glob import glob

# --- CONFIGURATION ---
RAW_DIR = "./Raw"
REF_DIR = os.path.join(RAW_DIR, "Reference")
OUTPUT_DIR = "../StockPredictor/Processed"
# ---------------------

def load_and_prep(file_path):
    df = pd.read_csv(file_path)
    if 'timestamp' in df.columns:
        df = df.rename(columns={'timestamp': 'time'})
    df.columns = [c.lower() for c in df.columns]
    df['time'] = pd.to_datetime(df['time'])
    return df.set_index('time').sort_index()

def add_time_feature(df):
    """
    Converts UTC index to NY time to handle DST shifts, 
    maps 09:30-16:00 to 0.0-1.0, then strips TZ info.
    """
    # 1. Localize to UTC, then convert to NY (Handles 4/5 hour DST shift)
    # Using 'America/New_York' as it's more standard than 'US/Eastern'
    eastern_times = df.index.tz_localize('UTC').tz_convert('America/New_York')
    
    # 2. Minutes from midnight in NY Time
    minutes = eastern_times.hour * 60 + eastern_times.minute
    
    # 3. Define standard NYSE bounds (Eastern Time)
    OPEN = 570  # 09:30
    CLOSE = 960 # 16:00
    
    # 4. Normalize and clip
    df['time_of_day'] = (minutes - OPEN) / (CLOSE - OPEN)
    df['time_of_day'] = df['time_of_day'].clip(0, 1).round(4)
    
    # Note: df.index remains UTC (tz-naive) because we calculated 
    # using a temporary series 'eastern_times'.
    return df

def process_all_stocks(debug=False):
    spy_path = os.path.join(REF_DIR, "SPY.csv")
    if not os.path.exists(spy_path):
        print(f"Error: Reference file not found at {spy_path}")
        return

    print("Loading SPY reference...")
    df_ref = load_and_prep(spy_path).add_prefix('spy_')

    all_csvs = glob(os.path.join(RAW_DIR, "**/*.csv"), recursive=True)
    target_files = [f for f in all_csvs if REF_DIR not in f]

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for file_path in target_files:
        ticker = os.path.basename(file_path).replace(".csv", "")
        print(f"Aligning {ticker} with SPY...")

        try:
            df_stock = load_and_prep(file_path)
            df_combined = df_stock.join(df_ref, how='inner')

            if df_combined.empty:
                print(f"  -> Skipping {ticker}: No overlap.")
                continue

            # Add features and ensure raw data types
            df_combined = add_time_feature(df_combined)
            
            # Ensure volumes are preserved as integers
            vol_cols = [c for c in df_combined.columns if 'volume' in c]
            for col in vol_cols:
                df_combined[col] = df_combined[col].fillna(0).astype('int64')

            # Round time_of_day for cleanliness, keep others raw
            df_combined['time_of_day'] = df_combined['time_of_day'].round(4)

            # Save Output
            if debug:
                out_path = os.path.join(OUTPUT_DIR, f"{ticker}_DEBUG.csv")
                df_combined.reset_index().to_csv(out_path, index=False)
            else:
                out_path = os.path.join(OUTPUT_DIR, f"{ticker}.parquet")
                df_combined.to_parquet(out_path)
            
            print(f"  -> Done! Saved {len(df_combined)} rows to {out_path}")

        except Exception as e:
            print(f"  -> Error processing {ticker}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process all stocks in Raw/ aligned to SPY.")
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()
    process_all_stocks(debug=args.debug)
