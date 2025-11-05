import requests
import pandas as pd
from ingestion.logging.metadata_log import log_metadata
from pathlib import Path
from config.config import INGESTION_DATA_DIR, ALTERNATIVEME_ID as ID

URL = "https://api.alternative.me/fng/"

params = {"limit": 365}

(INGESTION_DATA_DIR / ID).mkdir(parents=True, exist_ok=True)


def extract_data():
    r = requests.get(URL, params=params)
    data = r.json()
    return data

def extract_fear_and_greed():
    try:
        data = extract_data()
        data["data"]

        df = pd.DataFrame(data["data"])

        log_metadata(
        source="Alternative.me API",
        file_path="raw_fear_and_greed_idx.parquet",
        rows=len(df),
        notes="Fetched Fear and Greed Index data from Alternative.me API",
    )
    

        df.to_parquet("raw_fng_idx.parquet")
        df.to_parquet(INGESTION_DATA_DIR / ID / "raw_fng_idx.parquet")
        return f"{URL} OK"
    except:
        return f"{URL} ERROR"

if __name__ == "__main__":
    extract_fear_and_greed()
