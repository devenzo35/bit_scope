"""
This script transforms raw Coindesk article data, saves the result
as a parquet file, and loads it into the corresponding database table.
"""
import json
from pathlib import Path
import pandas as pd
from config.config import INGESTION_DATA_DIR, TRANSFORM_DATA_DIR, COINDESK_ID as ID
from storage.repositories import CoindeskArticlesRepository

# --- Configuration ---
SOURCE_PATH = INGESTION_DATA_DIR / ID / "raw_coindesk_articles.json"
DEST_DIR = TRANSFORM_DATA_DIR / ID
DEST_FILE_PATH = DEST_DIR / "coindesk_articles.parquet"

# Ensure the destination directory exists
DEST_DIR.mkdir(parents=True, exist_ok=True)


def transform_coindesk_articles():
    """
    Reads the raw article data, applies transformations, saves the result,
    and loads it into the database.
    """
    if not SOURCE_PATH.exists():
        raise FileNotFoundError(f"Raw file not found: {SOURCE_PATH}")

    with SOURCE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Handle structures with or without a 'data' key
    records = data.get("data", data)

    df = pd.DataFrame(records)

    # Select and rename columns
    if "title" in df.columns and "metadata" in df.columns:
        new_df = df[["title", "metadata"]].copy()
        new_df = new_df.rename(columns={"metadata": "extra_info"})
    else:
        # If the structure is different, create an empty df with expected columns
        new_df = pd.DataFrame(columns=["title", "extra_info"])

    # Ensure dtypes
    new_df["title"] = new_df["title"].astype("string")
    new_df["extra_info"] = new_df["extra_info"].astype("string")

    # Save processed result
    new_df.to_parquet(DEST_FILE_PATH, index=False)
    print(f"Processed file saved in: {DEST_FILE_PATH}")
    
    # --- Load to Database ---
    try:
        print("Loading data into database...")
        repo = CoindeskArticlesRepository()
        repo.create_tables_from_schema()
        repo.add_data(new_df)
    except Exception as e:
        print(f"Error loading data for {ID} into database: {e}")
    # --- End Load to Database ---

    return f"{ID} OK."
    
if __name__ == "__main__":
    transform_coindesk_articles()