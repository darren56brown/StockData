import os
import argparse
import glob
import subprocess
import sys

def get_all_tickers(base_dir, reference):
    """Scans for all files matching XYZ_yyyy-mm_5m.csv and extracts XYZ."""
    pattern = os.path.join(base_dir, "**/*_*_5m.csv")
    files = glob.glob(pattern, recursive=True)
    
    tickers = set()
    for f in files:
        filename = os.path.basename(f)
        # Expected format: TICKER_YYYY-MM_5m.csv
        parts = filename.split('_')
        if len(parts) >= 1:
            ticker = parts[0].upper()
            # Exclude the reference symbol itself
            if ticker != reference.upper():
                tickers.add(ticker)
    
    return sorted(list(tickers))

def run_all(args):
    tickers = get_all_tickers(args.dir, args.reference)
    
    if not tickers:
        print(f"No tickers found in {args.dir} (excluding {args.reference}).")
        return

    print(f"Found {len(tickers)} tickers to process: {', '.join(tickers)}")

    for ticker in tickers:
        print(f"\n--- Processing {ticker} ---")
        
        # Build the command to call the first script
        cmd = [
            sys.executable, "process_stock_data.py", ticker,
            "--reference", args.reference,
            "--dir", args.dir,
            "--out", args.out
        ]
        
        if args.debug:
            cmd.append("--debug")
        if not args.normalize:
            cmd.append("--no-normalize")
            
        # Execute the subprocess
        result = subprocess.run(cmd)
        
        if result.returncode != 0:
            print(f"Error: Processing failed for {ticker}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch process all stocks in a directory using process_stock_data.py.",
        epilog="Example: python process_all_stock_data.py --reference SPY --debug"
    )
    
    parser.add_argument("--reference", default="SPY", help="Reference symbol to exclude and align against (default: SPY)")
    parser.add_argument("--dir", default="./Raw", help="Source directory (default: ./Raw)")
    parser.add_argument("--out", default="./Processed", help="Output directory (default: ./Processed)")
    parser.add_argument("--debug", action="store_true", help="Save as CSV instead of Parquet")
    
    parser.add_argument("--normalize", action="store_true", default=True, help="Normalize (default)")
    parser.add_argument("--no-normalize", dest="normalize", action="store_false")

    if len(sys.argv) == 1 and "--help" not in sys.argv and "-h" not in sys.argv:
        # Default behavior: run on everything in ./Raw if no args provided
        args = parser.parse_args(["--dir", "./Raw"]) 
    else:
        args = parser.parse_args()

    run_all(args)
