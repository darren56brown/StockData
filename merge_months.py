import os
import glob
import pandas as pd
from collections import defaultdict

def consolidate_and_cleanup(root_dir='Raw'):
    # Find all CSV files recursively
    file_pattern = os.path.join(root_dir, '**', '*_*_5m.csv')
    all_files = glob.glob(file_pattern, recursive=True)

    # Group file paths by ticker and their specific directory
    # Key: (directory_path, ticker)
    groups = defaultdict(list)
    for file_path in all_files:
        directory = os.path.dirname(file_path)
        filename = os.path.basename(file_path)
        ticker = filename.split('_')[0]
        groups[(directory, ticker)].append(file_path)

    for (directory, ticker), files in groups.items():
        print(f"Merging {ticker} in {directory}...")
        
        # Sort files to maintain chronological order
        files.sort()
        
        # Combine data
        df_list = [pd.read_csv(f) for f in files]
        combined_df = pd.concat(df_list, ignore_index=True)
        
        # Save to the same directory
        output_path = os.path.join(directory, f"{ticker}.csv")
        combined_df.to_csv(output_path, index=False)
        
        # Delete the original monthly files
        for f in files:
            os.remove(f)
            
        print(f"Created {ticker}.csv and deleted {len(files)} source files.")

if __name__ == "__main__":
    consolidate_and_cleanup()
