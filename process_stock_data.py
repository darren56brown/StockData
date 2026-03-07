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
        
        # FIX: Map 'timestamp' to 'time' based on your sample data
        if 'timestamp' in df.columns:
            df = df.rename(columns={'timestamp': 'time'})
        
        # Standardize column names to lowercase to make the rest of the script cleaner
        df.columns = [c.lower() for c in df.columns]
        
        df['time'] = pd.to_datetime(df['time'])
        return df.set_index('time').sort_index()

    print(f"Aligning {ticker} with {reference} reference from {base_dir}...")
    df_stock = load_and_prep(ticker_files)
    df_ref = load_and_prep(ref_files).add_prefix(f'{reference.lower()}_')
    
    # Inner join handles the temporal overlap robustly
    df_combined = df_stock.join(df_ref, how='inner')

    if normalize:
        print(f"Applying Z-score normalization...")
        # We normalize all columns. 'time' is the index, so it is automatically excluded.
        df_combined = (df_combined - df_combined.mean()) / df_combined.std()

    os.makedirs(output_dir, exist_ok=True)
    
    if debug:
        out_path = os.path.join(output_dir, f"{ticker}_DEBUG.csv")
        # Reset index so 'time' appears as a column in your text editor
        df_combined.reset_index().to_csv(out_path, index=False)
    else:
        out_path = os.path.join(output_dir, f"{ticker}_PROD.parquet")
        df_combined.to_parquet(out_path)
    
    print(f"Done! Data contains {len(df_combined)} aligned rows.")
    print(f"Saved to: {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pre-process stock and reference market data for CNN training.",
        epilog="Example: python process_stock_data.py TSLA --reference SPY --debug"
    )
    
    parser.add_argument("ticker", help="Stock symbol (e.g. TSLA)")
    parser.add_argument("--reference", default="SPY", help="Reference symbol (default: SPY)")
    parser.add_argument("--dir", default="./Raw", help="Source directory (default: ./Raw)")
    parser.add_argument("--out", default="processed_data", help="Output directory")
    parser.add_argument("--debug", action="store_true", help="Save as CSV")
    
    parser.add_argument("--normalize", action="store_true", default=True, help="Normalize (default)")
    parser.add_argument("--no-normalize", dest="normalize", action="store_false")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    process_data(args.dir, args.ticker, args.reference, args.out, args.normalize, args.debug)
