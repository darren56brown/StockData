import os
import shutil
from pathlib import Path

def organize_staging_to_raw(base_dir="."):
    base_path = Path(base_dir)
    raw_dir = base_path / "Raw"
    staging_dir = base_path / "Staging"
    
    # 1. Build Ticker Map from Raw
    ticker_map = {} # {ticker: directory_path}
    
    for file_path in raw_dir.rglob("*.csv"):
        # Expecting format: XYZ_yyyy-mm_5m.csv
        parts = file_path.name.split('_')
        if len(parts) < 2:
            continue
            
        ticker = parts[0]
        current_dir = file_path.parent
        
        if ticker in ticker_map and ticker_map[ticker] != current_dir:
            print(f"ERROR: Ticker '{ticker}' found in multiple locations:")
            print(f"  - {ticker_map[ticker]}")
            print(f"  - {current_dir}")
            return
        
        ticker_map[ticker] = current_dir

    # 2. Process Staging Directory
    if not staging_dir.exists():
        print("Staging directory not found.")
        return

    for file_path in staging_dir.glob("*.csv"):
        name = file_path.name
        parts = name.split('_')
        
        # Identify month files (XYZ_yyyy-mm_5m.csv) 
        # Length of yyyy-mm is 7; length of yyyy-mm-dd is 10
        if len(parts) >= 2 and len(parts[1]) == 7:
            ticker = parts[0]
            
            if ticker in ticker_map:
                dest_path = ticker_map[ticker] / name
                
                if dest_path.exists():
                    print(f"OVERWRITING: {name} -> {dest_path}")
                else:
                    print(f"MOVING: {name} -> {dest_path}")
                
                shutil.move(str(file_path), str(dest_path))
            else:
                print(f"SKIPPING: Ticker '{ticker}' from {name} not found in Raw map.")
        
        # Daily files (len(parts[1]) == 10) are naturally ignored

if __name__ == "__main__":
    organize_staging_to_raw()
