import requests
import pandas as pd
from ingestion.logging.metadata_log import log_metadata
from pathlib import Path

URL = "https://api.alternative.me/fng/"
ID = "alternativeme_fear_and_greed"

params = {"limit": 365}

ROOTDIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOTDIR / "data" / "raw" / ID
DATA_DIR.mkdir(parents=True, exist_ok=True)

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

        df.to_parquet(DATA_DIR / "raw_fear_and_greed_idx.parquet")
        return f"{URL} OK"
    except:
        return f"{URL} ERROR"

if __name__ == "__main__":
    extract_fear_and_greed()
