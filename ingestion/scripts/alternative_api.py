"""
This script fetches the Fear & Greed Index from the Alternative.me API,
saves it as a parquet file, and logs metadata about the operation.
"""
import requests
import pandas as pd
from ingestion.logging.metadata_log import log_metadata
from pathlib import Path
from config.config import INGESTION_DATA_DIR, ALTERNATIVEME_ID as ID

# --- Configuration ---
URL = "https://api.alternative.me/fng/"
PARAMS = {"limit": 365}
DATA_DIR = INGESTION_DATA_DIR / ID
FILE_PATH = DATA_DIR / "raw_fng_idx.parquet"

# Ensure the destination directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)


def extract_data():
    """Makes the GET request to the API and returns the JSON data."""
    r = requests.get(URL, params=PARAMS)
    r.raise_for_status()  # Raise an exception for bad status codes
    return r.json()

def extract_fear_and_greed():
    """
    Extracts Fear and Greed Index data, saves it to a parquet file,
    and logs metadata.
    """
    try:
        data = extract_data()
        
        df = pd.DataFrame(data["data"])

        df.to_parquet(FILE_PATH)

        log_metadata(
            source="Alternative.me API",
            file_path=str(FILE_PATH),
            rows=len(df),
            notes="Fetched Fear and Greed Index data from Alternative.me API",
        )
        
        print(f"Successfully fetched and saved data from {URL}")
        return f"{URL} OK"
    except requests.exceptions.RequestException as e:
        print(f"ERROR fetching data from {URL}: {e}")
        return f"{URL} ERROR"
    except (KeyError, pd.errors.EmptyDataError) as e:
        print(f"ERROR processing data: {e}")
        return f"{URL} ERROR"

if __name__ == "__main__":
    extract_fear_and_greed()
