import pandas as pd
import requests
import time
from ingestion.logging.metadata_log import log_metadata
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import logging

load_dotenv()

# btc metadata url = f'https://api.coingecko.com/api/v3/coins/bitcoin'
to_timestamp = int(time.time())
from_date = datetime.now(timezone.utc) - timedelta(days=365)
from_timestamp = from_date.timestamp()

API_KEY = os.getenv("COINGECKO_API_KEY")
URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
params: dict[str, str | float | int] = {
    "vs_currency": "usd",
    "from": from_timestamp,
    "to": to_timestamp,
}


def data_extract():
    try:
        r = requests.get(URL, params=params, headers={"x-cg-demo-api-key": API_KEY})
        r.raise_for_status()
        data = r.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def main():
    try:
        logging.info("Starting BTC price ingestion process.")

        data = data_extract()
        df = pd.DataFrame(data, dtype=object)

        df["date"] = [elem[0] for elem in df["prices"]]
        df["date"] = pd.to_datetime(df["date"], unit="ms")

        df.set_index("date", inplace=True)

        df["prices"] = [round(elem[1], 2) for elem in df["prices"]]
        df["market_caps"] = [round(elem[1], 2) for elem in df["market_caps"]]
        df["total_volumes"] = [round(elem[1], 2) for elem in df["total_volumes"]]

        df["prices"].plot()

        log_metadata(
            source="CoinGecko API",
            file_path="raw_btc_prices.parquet",
            rows=len(df),
            notes="Data fetched from CoinGecko API, stored as parquet file, containing BTC prices, market caps, and total volumes.",
        )

        df.to_parquet("raw_btc_prices.parquet")
        logging.info("BTC price ingestion process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during the BTC price ingestion process: {e}")


if __name__ == "__main__":
    main()
