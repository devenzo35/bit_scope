import requests
import pandas as pd
from ingestion.logging.metadata_log import log_metadata

URL = "https://api.alternative.me/fng/"

params = {"limit": 365}


def extract_data():
    r = requests.get(URL, params=params)
    data = r.json()
    return data


def main():
    data = extract_data()
    data["data"]

    df = pd.DataFrame(data["data"])

    log_metadata(
        source="Alternative.me API",
        file_path="raw_fear_and_greed_idx.parquet",
        rows=len(df),
        notes="Fetched Fear and Greed Index data from Alternative.me API",
    )

    df.to_parquet("raw_fear_and_greed_idx.parquet")


if __name__ == "__main__":
    main()
