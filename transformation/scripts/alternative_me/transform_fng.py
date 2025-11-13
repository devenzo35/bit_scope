"""
This script transforms the raw Fear & Greed Index data, saves the result
as a parquet file, and loads it into the corresponding database table.
"""
import pandas as pd
import datetime as dt
import numpy as np
from config.config import INGESTION_DATA_DIR, TRANSFORM_DATA_DIR, ALTERNATIVEME_ID as ID
from storage.repositories import FearAndGreedRepository

# --- Configuration ---
SOURCE_PATH = INGESTION_DATA_DIR / ID / "raw_fng_idx.parquet"
DEST_DIR = TRANSFORM_DATA_DIR / ID
DEST_FILE_PATH = DEST_DIR / "transform_fng_idx.parquet"

# Ensure the destination directory exists
DEST_DIR.mkdir(parents=True, exist_ok=True)


def transform_fng():
    """
    Reads the raw F&G data, applies transformations, saves the result,
    and loads it into the database.
    """
    # Your original transformation logic
    df = pd.read_parquet(SOURCE_PATH)

    df['timestamp'] = df['timestamp'].astype(int)
    df['date'] = pd.to_datetime(df['timestamp'], unit="s")

    df['time_until_update'] = pd.to_numeric(df['time_until_update'], errors="coerce")
    df['hours_until_update'] = (df['time_until_update'] / 3600).round(2)

    df.loc[df['time_until_update'] < 0, "time_until_update"] = np.nan

    df.set_index('date', inplace=True)

    df = df[['value', 'value_classification',
            #'timestamp', 'time_until_update',
            'hours_until_update']]

    df.to_parquet(DEST_FILE_PATH)
    
    # --- Load to Database ---
    try:
        print("Loading data into database...")
        repo = FearAndGreedRepository()
        repo.create_tables_from_schema()
        # The DataFrame has 'date' as its index; reset it for saving.
        df_to_save = df.reset_index()
        repo.add_data(df_to_save)
    except Exception as e:
        print(f"Error loading data for {ID} into database: {e}")
    # --- End Load to Database ---
    
    return f"{ID} OK."
  

if __name__ == "__main__":
  transform_fng()