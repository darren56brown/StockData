import subprocess

# Hardcode your lists here
tickers = [
    'AMD', 
    'AVGO', 
    'PLTR', 
    'MU'
    ]
months = ['2026-01', '2026-02']

for ticker in tickers:
    for month in months:
        print(f"Running: {ticker} for {month}...")
        
        # This executes: python yahoo_monthly_pull.py <ticker> <yyyy-mm> 5m
        subprocess.run(['python', 'yahoo_monthly_pull.py', ticker, month, '5m'])

print("Done!")
