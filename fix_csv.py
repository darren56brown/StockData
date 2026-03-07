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
        content = list(csv.reader(f))
    
    if len(content) <= 1:
        return

    header = content[0]
    data = content[1:]

    # 1. Deduplicate perfectly identical adjacent rows
    data = [key for key, _ in itertools.groupby(data)]
    
    # 2. Logic to drop rows with 0 volume if they share a timestamp/price with a neighbor
    # Assuming Column 0 is Timestamp and Column 5 is Volume (Adjust index if needed)
    VOL_IDX = 5
    TS_IDX = 0
    
    filtered_data = []
    if data:
        filtered_data.append(data[0])
        for i in range(1, len(data)):
            prev = filtered_data[-1]
            curr = data[i]
            
            # Check if they share the same timestamp
            if curr[TS_IDX] == prev[TS_IDX]:
                curr_vol = curr[VOL_IDX].strip()
                prev_vol = prev[VOL_IDX].strip()
                
                # If current is 0 and previous isn't, skip current
                if curr_vol == "0" and prev_vol != "0":
                    continue
                # If previous was 0 and current isn't, replace previous
                elif prev_vol == "0" and curr_vol != "0":
                    filtered_data[-1] = curr
                    continue
            
            filtered_data.append(curr)

    lines_removed = len(content[1:]) - len(filtered_data)
    
    # 3. Safety Checks (on the filtered data)
    collision_timestamps = []
    out_of_order = 0
    
    for i in range(1, len(filtered_data)):
        prev_ts = filtered_data[i-1][TS_IDX]
        curr_ts = filtered_data[i][TS_IDX]
        
        if curr_ts == prev_ts:
            collision_timestamps.append(curr_ts)
        elif curr_ts < prev_ts:
            out_of_order += 1

    # 4. Save if changes were made
    if lines_removed > 0:
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(filtered_data)
        
        print(f"MODIFIED: {path}")
        print(f"  -> Removed {lines_removed} lines (Identical or 0-volume duplicates).")
    
    # 5. Enhanced Warnings
    if collision_timestamps or out_of_order > 0:
        if lines_removed == 0: print(f"CHECK:    {path}")
        
        if collision_timestamps:
            ts_display = ", ".join(collision_timestamps[:5])
            more = f" (+{len(collision_timestamps)-5} more)" if len(collision_timestamps) > 5 else ""
            print(f"  [!] WARNING: {len(collision_timestamps)} duplicate timestamps remain at: {ts_display}{more}")
            
        if out_of_order > 0:
            print(f"  [!] WARNING: {out_of_order} timestamps appear out of chronological order.")

if __name__ == "__main__":
    process_files('./Raw')
