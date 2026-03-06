import os
import csv
import itertools

def process_files(root_dir):
    print(f"Scanning: {os.path.abspath(root_dir)}\n" + "="*50)
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.lower().endswith('.csv'):
                clean_and_verify(os.path.join(root, file))

def clean_and_verify(path):
    with open(path, 'r', newline='', encoding='utf-8') as f:
        reader = list(csv.reader(f))
    
    if len(reader) <= 1:
        return

    header = reader[0]
    data = reader[1:]

    # 1. Deduplicate identical adjacent rows
    # key=lambda x: x ensures the entire row content is compared
    clean_data = [key for key, _ in itertools.groupby(data)]
    
    lines_removed = len(data) - len(clean_data)
    
    # 2. Safety Checks (Warnings only)
    ts_collisions = 0
    out_of_order = 0
    
    for i in range(1, len(clean_data)):
        prev_ts = clean_data[i-1][0]
        curr_ts = clean_data[i][0]
        
        if curr_ts == prev_ts:
            ts_collisions += 1
        elif curr_ts < prev_ts:
            out_of_order += 1

    # 3. Save if changes were made and echo summary
    if lines_removed > 0:
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(clean_data)
        
        print(f"MODIFIED: {path}")
        print(f"  -> Removed {lines_removed} identical lines.")
    
    # Always echo warnings if issues are found, even if no lines were removed
    if ts_collisions > 0 or out_of_order > 0:
        if lines_removed == 0: print(f"CHECK:    {path}")
        if ts_collisions > 0: print(f"  [!] WARNING: {ts_collisions} duplicate timestamps with DIFFERENT values.")
        if out_of_order > 0:  print(f"  [!] WARNING: {out_of_order} timestamps appear out of chronological order.")

if __name__ == "__main__":
    process_files('.') # Set to your repo root

