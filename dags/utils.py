import pandas as pd
import argparse
from datetime import datetime, timedelta
import re

# Get S&P 500 tickers from Wikipedia
# ------------------ Functions ------------------------
def get_sp500_tickers() -> list[str]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    # Add a User-Agent so Wikipedia doesn't block the request
    dfs = pd.read_html(
        url,
        match="Symbol",
        storage_options={"User-Agent": "Mozilla/5.0"}
    )
    df = dfs[0]

    tickers = (
        df["Symbol"]
        .astype(str)
        .str.strip()
        .dropna()
        .drop_duplicates()
        .apply(lambda t: re.sub(r"\.", "-", t))  # Replace dots with dashes
        .sort_values()  # Sort alphabetically
        .tolist()
    )
    return tickers

# Get target date from command line arguments
def get_target_date():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', help='Run for specific date (YYYY-MM-DD)')

    # In Jupyter, ignore extra arguments (like `--f`)
    args, unknown = parser.parse_known_args()

    # Try to parse the date or default to yesterday
    try:
        if args.date:
            # Validate format
            datetime.strptime(args.date, "%Y-%m-%d")
            return args.date
    except Exception:
        print(f"[WARN] Invalid --date format, defaulting to yesterday.")

    # Default fallback
    target = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[INFO] No valid --date provided. Defaulting to: {target}")
    return target

# Check if a date is a weekend
def is_weekend(date_str: str) -> bool:
    """Return True if the date is Saturday or Sunday."""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj.weekday() >= 5  # 5 = Saturday, 6 = Sunday

from pathlib import Path

def log_failed_fetch(symbol, date, reason):
    log_file = Path("logs/failed_fetches.txt")
    log_file.parent.mkdir(exist_ok=True)  # Create 'logs' folder if needed

    with log_file.open("a") as f:
        f.write(f"{symbol},{date},{reason}\n")