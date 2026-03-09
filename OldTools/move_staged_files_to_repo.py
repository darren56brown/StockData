import os
import shutil
from pathlib import Path

def organize_staging_to_raw(base_dir="."):
    base_path = Path(base_dir)
    raw_dir = base_path / "Raw"
    staging_dir = base_path / "Staging"
    
    # Counters for final echo
    total_moved = 0
    total_overwritten = 0
    
    # 1. Build Ticker Map from Raw
    ticker_map = {} # {ticker: directory_path}
    
    for file_path in raw_dir.rglob("*.csv"):
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
        
        # Month files have date format YYYY-MM (length 7)
        # Daily files have date format YYYY-MM-DD (length 10)
        if len(parts) >= 2 and len(parts[1]) == 7:
            ticker = parts[0]
            
            if ticker in ticker_map:
                dest_path = ticker_map[ticker] / name
                
                if dest_path.exists():
                    print(f"OVERWRITING: {name} -> {dest_path}")
                    total_overwritten += 1
                else:
                    print(f"MOVING: {name} -> {dest_path}")
                
                shutil.move(str(file_path), str(dest_path))
                total_moved += 1
            else:
                print(f"SKIPPING: Ticker '{ticker}' from {name} not found in Raw map.")

    # 3. Final Echo
    print("\n" + "="*30)
    print(f"Total files moved: {total_moved}")
    print(f"Total files overwritten: {total_overwritten}")
    print("="*30)

if __name__ == "__main__":
    organize_staging_to_raw()
