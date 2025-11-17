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

    df = pd.json_normalize(data['data'])
    
    df.rename(columns={'num_comments': 'comments_count'}, inplace=True)
    
    df = df[['id',
        #'name',
        'title', 'selftext',
        #'author', 'author_flair_text','clicked',
        'comments_count',
        #'distinguished', 'edited',
       #'is_original_content', 'is_self', 'link_flair_template_id',
       'link_flair_text',
         #'locked',
        #'over_18',
        'permalink',
        #'saved',
        'score',
       #'spoiler', 'stickied',
        'subreddit', 'upvote_ratio', 'url',
       'created_utc']]
    
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s', utc=True)

    df.to_json(DEST_FILE_PATH)
  
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