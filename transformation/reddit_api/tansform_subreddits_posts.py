import pandas as pd
import json
from config.config import INGESTION_DATA_DIR,TRANSFORM_DATA_DIR, REDDIT_API_ID as ID

DEST_DIR = TRANSFORM_DATA_DIR / ID
(DEST_DIR).mkdir(parents=True, exist_ok=True)

with open(INGESTION_DATA_DIR / ID / 'raw_subreddits_posts.json') as f:
  data = json.load(f)

df = pd.json_normalize(data['data'])

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

df.to_parquet(DEST_DIR / 'transform_subreddits_posts.parquet')