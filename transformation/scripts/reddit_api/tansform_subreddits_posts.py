import pandas as pd
import json
from config.config import INGESTION_DATA_DIR,TRANSFORM_DATA_DIR, REDDIT_ID as ID

DEST_DIR = TRANSFORM_DATA_DIR / ID
(DEST_DIR).mkdir(parents=True, exist_ok=True)

from storage.repositories import RedditPostsRepository

def transform_subreddits_posts():
    """
    Transforms the raw Reddit posts data and loads it into the database.
    """
    try:
        # Transformación
        with open(INGESTION_DATA_DIR / ID / 'raw_subreddits_posts.json') as f:
            data = json.load(f)

        df = pd.json_normalize(data)

        # Seleccionar y renombrar columnas para que coincidan con el schema.sql
        df_transformed = df[[
            'id', 'title', 'author', 'created_utc', 'score', 'upvote_ratio', 
            'full_link', 'num_comments', 'num_crossposts', 'total_awards_received', 
            'selftext', 'subreddit', 'subreddit_subscribers'
        ]].copy()

        df_transformed['created_utc'] = pd.to_datetime(df_transformed['created_utc'], unit='s')
        
        # Llenar valores nulos en 'selftext' para evitar problemas con la base de datos
        df_transformed['selftext'].fillna('', inplace=True)

        # Guardar en Parquet
        df_transformed.to_parquet(DEST_DIR / 'transform_subreddits_posts.parquet', index=False)

        # Carga a la base de datos
        print("Loading transformed Reddit posts into the database...")
        repo = RedditPostsRepository()
        repo.create_tables_from_schema()
        repo.add_posts(df_transformed)

        return f"Transformed and loaded {len(df_transformed)} records for {ID} successfully."

    except Exception as e:
        return f"Error during transformation/loading for {ID}: {e}"
  
  
if __name__ == "__main__":
  transform_subreddits_posts()