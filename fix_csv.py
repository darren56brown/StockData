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
    clean_data = [key for key, _ in itertools.groupby(data)]
    lines_removed = len(data) - len(clean_data)
    
    # 2. Safety Checks
    collision_timestamps = []
    out_of_order = 0
    
    for i in range(1, len(clean_data)):
        prev_ts = clean_data[i-1][0]
        curr_ts = clean_data[i][0]
        
        if curr_ts == prev_ts:
            collision_timestamps.append(curr_ts)
        elif curr_ts < prev_ts:
            out_of_order += 1

    # 3. Save if changes were made
    if lines_removed > 0:
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(clean_data)
        
        print(f"MODIFIED: {path}")
        print(f"  -> Removed {lines_removed} identical lines.")
    
    # 4. Enhanced Warnings with Timestamp reporting
    if collision_timestamps or out_of_order > 0:
        if lines_removed == 0: print(f"CHECK:    {path}")
        
        if collision_timestamps:
            # Showing up to 5 specific timestamps to keep output clean
            ts_display = ", ".join(collision_timestamps[:5])
            more = f" (+{len(collision_timestamps)-5} more)" if len(collision_timestamps) > 5 else ""
            print(f"  [!] WARNING: {len(collision_timestamps)} duplicate timestamps at: {ts_display}{more}")
            
        if out_of_order > 0:
            print(f"  [!] WARNING: {out_of_order} timestamps appear out of chronological order.")

if __name__ == "__main__":
    # Ensure the 'Raw' folder exists or change this path to your target directory
    target_dir = './Raw'
    if os.path.exists(target_dir):
        process_files(target_dir)
    else:
        print(f"Directory '{target_dir}' not found.")
