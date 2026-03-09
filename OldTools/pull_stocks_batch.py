import subprocess
from tools import get_all_tickers


def run_all(dir):
    start_day = 1
    end_day = 31

    #tickers = ['XXX', 'XXX', 'XXX', 'XXX', 'XXX', 
    #    'XXX', 'XXX', 'XXX', 'XXX', 'XXX']
    #months = ['2026-01', '2026-02']
    
    tickers = get_all_tickers(dir, "")
    months = ['2026-03']
    start_day = 1
    end_day = 8

    if not tickers:
        print(f"No tickers found in {dir}.")
        return

    print(f"Found {len(tickers)} tickers to process: {', '.join(tickers)}")

    for ticker in tickers:
        for month in months:
            print(f"Running: {ticker} for {month}...")
            
            # This executes: python yahoo_monthly_pull.py <ticker> <yyyy-mm> 5m
            subprocess.run(['python', 'yahoo_monthly_pull.py', ticker, month, '5m',
                            '--start-day', str(start_day), '--end-day', str(end_day)])

    print("Done!")


if __name__ == "__main__":
    run_all("./Raw")