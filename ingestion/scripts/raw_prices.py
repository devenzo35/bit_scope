"""
This script fetches the historical market chart for Bitcoin from the CoinGecko API,
saves it as a parquet file, and logs metadata about the operation.
"""
import pandas as pd
import requests
import time
from ingestion.logging.metadata_log import log_metadata
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
from pathlib import Path
from config.config import INGESTION_DATA_DIR, COINGECKO_ID as ID

# --- Configuration ---
load_dotenv()
API_KEY = os.getenv("COINGECKO_API_KEY")
URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"

# Calculate time range for the API request (last 365 days)
to_timestamp = int(time.time())
from_timestamp = (datetime.now(timezone.utc) - timedelta(days=365)).timestamp()

PARAMS = {
    "vs_currency": "usd",
    "from": from_timestamp,
    "to": to_timestamp,
}
DATA_DIR = INGESTION_DATA_DIR / ID
FILE_PATH = DATA_DIR / "raw_btc_prices.parquet"

# Ensure the destination directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)


def data_extract() -> dict:
    """
    Makes the GET request to the CoinGecko API and returns the JSON data.
    """
    if not API_KEY:
        raise ValueError("COINGECKO_API_KEY not found in environment variables.")
    
    headers = {"x-cg-demo-api-key": API_KEY}
    r = requests.get(URL, params=PARAMS, headers=headers)
    r.raise_for_status()
    return r.json()


def extract_raw_prices():
    """
    Extracts BTC price data, saves it to a parquet file, and logs metadata.
    """
    print("Starting BTC price ingestion process.")
    try:
        data = data_extract()
        df = pd.DataFrame(data)

        df.to_parquet(FILE_PATH)

        log_metadata(
            source="CoinGecko API",
            file_path=str(FILE_PATH),
            rows=len(df),
            notes="Fetched BTC prices, market caps, and total volumes.",
        )

        print("BTC price ingestion process completed successfully.")
        return f"{URL} OK"
    except Exception as e:
        print(f"An error occurred during the BTC price ingestion process: {e}")
        return f"{URL} ERROR"


if __name__ == "__main__":
    extract_raw_prices()
