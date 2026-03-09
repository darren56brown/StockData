
import glob
import os

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