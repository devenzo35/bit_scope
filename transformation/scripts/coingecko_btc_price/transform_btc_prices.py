"""
This script transforms raw Bitcoin price data from CoinGecko, saves the result
as a parquet file, and loads it into the corresponding database table.
"""
import pandas as pd
from pathlib import Path
from config.config import INGESTION_DATA_DIR, TRANSFORM_DATA_DIR, COINGECKO_ID as ID
from storage.repositories import BtcPricesRepository

# --- Configuration ---
SOURCE_PATH = INGESTION_DATA_DIR / ID / "raw_btc_prices.parquet"
DEST_DIR = TRANSFORM_DATA_DIR / ID
DEST_FILE_PATH = DEST_DIR / "btc_prices.parquet"

# Ensure the destination directory exists
DEST_DIR.mkdir(parents=True, exist_ok=True)


def transform_btc_prices():
    """
    Reads the raw BTC price data, applies transformations, saves the result,
    and loads it into the database.
    """
    df = pd.read_parquet(SOURCE_PATH)

    df["date"] = [elem[0] for elem in df["prices"]]
    df["date"] = pd.to_datetime(df["date"], unit="ms")

    df.set_index("date", inplace=True)

    df["prices"] = [round(elem[1], 2) for elem in df["prices"]]
    df["market_caps"] = [round(elem[1], 2) for elem in df["market_caps"]]
    df["total_volumes"] = [round(elem[1], 2) for elem in df["total_volumes"]]

    df.rename(columns={"prices":"price",  "market_caps":"market_cap", "total_volumes":"total_volume" }, inplace=True)

    df = df.dropna(subset=['price'])
    df = df.dropna(subset=['market_cap'])
    df = df.dropna(subset=['total_volume'])

    assert (df['price'] > 0).all(), "Found positive prices"

    is_empty = df.loc[df['market_cap'].duplicated()]
    is_empty.empty

    df.dtypes

    df.describe()

    df.to_parquet(DEST_FILE_PATH)
    
    # --- Load to Database ---
    try:
        print("Loading data into database...")
        repo = BtcPricesRepository()
        repo.create_tables_from_schema()
        # The DataFrame has 'date' as its index; reset it for saving.
        df_to_save = df.reset_index()
        repo.add_data(df_to_save)
    except Exception as e:
        print(f"Error loading data for {ID} into database: {e}")
    # --- End Load to Database ---

    return f"{ID} OK."
    
if __name__ == "__main__":
    transform_btc_prices()