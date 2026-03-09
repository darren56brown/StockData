import os
import argparse
import pandas as pd
from glob import glob
import sys

def process_data(base_dir, ticker, reference, output_dir, normalize=True, debug=False):
    # Search for files recursively
    ticker_pattern = os.path.join(base_dir, f"**/{ticker}_*_5m.csv")
    ref_pattern = os.path.join(base_dir, f"**/{reference}_*_5m.csv")
    
    ticker_files = sorted(glob(ticker_pattern, recursive=True))
    ref_files = sorted(glob(ref_pattern, recursive=True))
    
    if not ticker_files or not ref_files:
        print(f"Error: Missing files for {ticker} or {reference} in {base_dir}")
        return

    def load_and_prep(files):
        df = pd.concat([pd.read_csv(f) for f in files])
        
        # Map 'timestamp' to 'time'
        if 'timestamp' in df.columns:
            df = df.rename(columns={'timestamp': 'time'})
        
        # Standardize column names to lowercase
        df.columns = [c.lower() for c in df.columns]
        
        df['time'] = pd.to_datetime(df['time'])
        return df.set_index('time').sort_index()

    print(f"Aligning {ticker} with {reference} reference...")
    df_stock = load_and_prep(ticker_files)
    df_ref = load_and_prep(ref_files).add_prefix(f'{reference.lower()}_')
    
    # Inner join for perfect temporal overlap
    df_combined = df_stock.join(df_ref, how='inner').astype('float32')

    if normalize:
        print(f"Applying Z-score normalization...")
        # Normalize and round to 6 decimal places to clean up the ASCII/CSV output
        df_combined = ((df_combined - df_combined.mean()) / df_combined.std()).round(6)

    os.makedirs(output_dir, exist_ok=True)
    
    if debug:
        out_path = os.path.join(output_dir, f"{ticker}_DEBUG.csv")
        df_combined.reset_index().to_csv(out_path, index=False)
    else:
        out_path = os.path.join(output_dir, f"{ticker}.parquet")
        df_combined.to_parquet(out_path)
    
    print(f"Done! Saved {len(df_combined)} rows to: {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pre-process stock and reference market data for CNN training.",
        epilog="Example: python process_stock_data.py TSLA --debug"
    )
    
    parser.add_argument("ticker", help="Stock symbol (e.g. TSLA)")
    parser.add_argument("--reference", default="SPY", help="Reference symbol (default: SPY)")
    parser.add_argument("--dir", default="./Raw", help="Source directory (default: ./Raw)")
    parser.add_argument("--out", default="./Processed", help="Output directory (default: ./Processed)")
    parser.add_argument("--debug", action="store_true", help="Save as CSV with readable precision")
    
    parser.add_argument("--normalize", action="store_true", default=True, help="Normalize (default)")
    parser.add_argument("--no-normalize", dest="normalize", action="store_false")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    process_data(args.dir, args.ticker, args.reference, args.out, args.normalize, args.debug)
