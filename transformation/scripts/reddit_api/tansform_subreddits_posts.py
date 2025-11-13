"""
This script transforms raw Reddit posts data, saves the result
as a parquet file, and loads it into the corresponding database table.
"""
import pandas as pd
import json
from config.config import INGESTION_DATA_DIR, TRANSFORM_DATA_DIR, REDDIT_ID as ID
from storage.repositories import RedditPostsRepository

# --- Configuration ---
SOURCE_PATH = INGESTION_DATA_DIR / ID / 'raw_subreddits_posts.json'
DEST_DIR = TRANSFORM_DATA_DIR / ID
DEST_FILE_PATH = DEST_DIR / 'transform_subreddits_posts.parquet'

# Ensure the destination directory exists
DEST_DIR.mkdir(parents=True, exist_ok=True)


def transform_subreddits_posts():
    """
    Reads the raw Reddit posts data, applies transformations, saves the result,
    and loads it into the database.
    """
    with open(SOURCE_PATH) as f:
        data = json.load(f)

    # Your original transformation logic
    df = pd.json_normalize(data) # Adjusted to handle the new raw format

    df.info()

    missing_values = df.isnull().sum()
    missing_percentage = (df.isnull().sum() / len(df)) * 100

    missing_info = pd.DataFrame({
        'Missing Count': missing_values,
        'Missing Percentage': missing_percentage
    })

    missing_info = missing_info.sort_values(by='Missing Percentage', ascending=False)
    print("Missing Values Information:")
    print(missing_info[missing_info['Missing Count'] > 0])

    # Note: The columns you selected are slightly different from what the ingestion
    # script now provides. I will select the available columns from your list.
    available_cols = [
        'id', 'title', 'selftext', 'num_comments', 'link_flair_text', 
        'permalink', 'score', 'subreddit', 'upvote_ratio', 'url', 'created_utc'
    ]
    # The ingestion script now provides 'num_comments' instead of 'comments_count'
    df.rename(columns={'num_comments': 'comments_count'}, inplace=True)
    
    df = df[[col for col in available_cols if col in df.columns]]

    df.to_parquet(DEST_FILE_PATH)
  
    # --- Load to Database ---
    try:
        print("Loading data into database...")
        repo = RedditPostsRepository()
        repo.create_tables_from_schema()
        # Fill NaNs in selftext to avoid database issues
        df_to_save = df.copy()
        df_to_save['selftext'].fillna('', inplace=True)
        repo.add_data(df_to_save)
    except Exception as e:
        print(f"Error loading data for {ID} into database: {e}")
    # --- End Load to Database ---

    return f"{ID} OK."
  
  
if __name__ == "__main__":
    transform_subreddits_posts()