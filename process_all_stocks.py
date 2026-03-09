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

def process_all_stocks(normalize=True, debug=False):
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

            if normalize:
                # Cast to float for Z-score calculation
                df_combined = df_combined.astype('float32')
                df_combined = ((df_combined - df_combined.mean()) / df_combined.std()).round(6)
            else:
                # Sanity check mode: Keep prices as floats and volumes as integers
                # Find any column that contains 'volume'
                vol_cols = [c for c in df_combined.columns if 'volume' in c]
                for col in vol_cols:
                    df_combined[col] = df_combined[col].fillna(0).astype('int64')

            # Save Output
            if debug:
                out_path = os.path.join(OUTPUT_DIR, f"{ticker}_DEBUG.csv")
                # float_format=None prevents e-notation for standard floats in CSV
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
    parser.add_argument("--normalize", action="store_true", default=True)
    parser.add_argument("--no-normalize", dest="normalize", action="store_false")

    args = parser.parse_args()
    process_all_stocks(normalize=args.normalize, debug=args.debug)
