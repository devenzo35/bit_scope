"""
This script fetches multiple economic data series from the FRED API,
saving each series as a separate parquet file and logging metadata for each.
"""
import pandas as pd
from fredapi import Fred
from ingestion.logging.metadata_log import log_metadata
from pathlib import Path
from config.config import INGESTION_DATA_DIR, FRED_ID as ID
from dotenv import load_dotenv
import os

# --- Configuration ---
load_dotenv()
API_KEY = os.getenv("FRED_API_KEY")
DATA_DIR = INGESTION_DATA_DIR / ID

# A dictionary mapping series IDs to their file names and descriptions
SERIES_TO_FETCH = {
    "DFEDTARU": "raw_interest_rates.parquet",
    "SP500": "raw_sp500_series.parquet",
    "CPIAUCSL": "raw_cpi.parquet",
    "UNRATE": "raw_unemp_rate.parquet",
}

# Ensure the destination directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)


def fetch_and_save_series(fred: Fred, series_id: str, file_name: str):
    """
    Fetches a single data series from FRED, saves it as a parquet file,
    and logs metadata.

    Args:
        fred: An authenticated fredapi.Fred instance.
        series_id: The ID of the data series to fetch.
        file_name: The name of the file to save the data to.
    """
    print(f"Fetching series '{series_id}'...")
    try:
        series_data = fred.get_series(series_id)
        df = pd.DataFrame(series_data)
        
        file_path = DATA_DIR / file_name
        df.to_parquet(file_path)

        log_metadata(
            source=f"FRED API ({series_id})",
            file_path=str(file_path),
            rows=len(df),
            notes=f"Fetched {series_id} data from FRED API.",
        )
        print(f"Successfully saved '{series_id}' to {file_name}")
    except Exception as e:
        print(f"Failed to fetch or save series '{series_id}': {e}")
        raise  # Re-raise the exception to be caught by the main function


def extract_fred_api():
    """
    Main function to extract all specified data series from the FRED API.
    """
    if not API_KEY:
        print("ERROR: FRED_API_KEY not found. Please set it in your .env file.")
        return f"{ID} ERROR"

    try:
        fred = Fred(api_key=API_KEY)
        for series_id, file_name in SERIES_TO_FETCH.items():
            fetch_and_save_series(fred, series_id, file_name)
        
        return f"{ID} OK"
    except Exception as e:
        print(f"An error occurred during the FRED API extraction process: {e}")
        return f"{ID} ERROR"


if __name__ == "__main__":
    extract_fred_api()
