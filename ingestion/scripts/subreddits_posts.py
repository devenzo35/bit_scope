"""
This script fetches recent posts from multiple subreddits using the Reddit API (asyncpraw),
extracts relevant fields, and saves the data as a single JSON file.
"""
import asyncio
import asyncpraw
from datetime import datetime, timezone
import json
from pathlib import Path
import os
from dotenv import load_dotenv
from config.config import INGESTION_DATA_DIR, REDDIT_ID as ID

# --- Configuration ---
load_dotenv()
DATA_DIR = INGESTION_DATA_DIR / ID
FILE_PATH = DATA_DIR / "raw_subreddits_posts.json"
SUBREDDITS_TO_FETCH = "Bitcoin+btc+CryptoCurrency"
POST_LIMIT = 1000

# Ensure the destination directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)


def extract_submission_data(submission) -> dict:
    """
    Extracts all relevant fields from a PRAW submission object
    and returns them as a Python dictionary.
    """
    return {
        "id": submission.id,
        "title": submission.title,
        "selftext": submission.selftext,
        "author": str(submission.author) if submission.author else None,
        "author_flair_text": submission.author_flair_text,
        "clicked": submission.clicked,
        "num_comments": submission.num_comments,
        "distinguished": submission.distinguished,
        "edited": submission.edited,
        "is_original_content": submission.is_original_content,
        "is_self": submission.is_self,
        "link_flair_text": submission.link_flair_text,
        "locked": submission.locked,
        "over_18": submission.over_18,
        "permalink": submission.permalink,
        "full_link": f"https://www.reddit.com{submission.permalink}",
        "saved": submission.saved,
        "score": submission.score,
        "spoiler": submission.spoiler,
        "stickied": submission.stickied,
        "subreddit": str(submission.subreddit),
        "subreddit_subscribers": getattr(submission.subreddit, 'subscribers', None),
        "upvote_ratio": submission.upvote_ratio,
        "url": submission.url,
        "created_utc": submission.created_utc,
        "num_crossposts": getattr(submission, 'num_crossposts', 0),
        "total_awards_received": getattr(submission, 'total_awards_received', 0),
    }


async def extract_subreddits():
    """
    Main asynchronous function to orchestrate the fetching and saving of Reddit posts.
    """
    print("Starting Reddit post ingestion...")
    client_id = os.getenv("REDDIS_CLIENT_ID")
    client_secret = os.getenv("REDDIS_CLIENT_SECRET")
    user_agent = os.getenv("REDDIS_USER_AGENT")

    if not all([client_id, client_secret, user_agent]):
        print("ERROR: Reddit API credentials not found in environment variables.")
        return f"{ID} ERROR"

    try:
        async with asyncpraw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        ) as reddit:
            subreddits = await reddit.subreddit(SUBREDDITS_TO_FETCH)
            posts = []
            print(f"Fetching up to {POST_LIMIT} new posts from '{SUBREDDITS_TO_FETCH}'...")
            async for submission in subreddits.new(limit=POST_LIMIT):
                posts.append(extract_submission_data(submission))

            print(f"Successfully fetched {len(posts)} posts.")
            
            data = {
            "metadata": {
                "source": "Reddit API",
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
            },
            "data": posts,
            }

            # Save data to JSON file
            with open(FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            
            print(f"Data saved to {FILE_PATH}")
            return f"{ID} OK"

    except Exception as e:
        print(f"An error occurred during Reddit ingestion: {e}")
        return f"{ID} ERROR"


if __name__ == "__main__":
    # In Python 3.7+, asyncio.run is the standard way to run an async function.
    asyncio.run(extract_subreddits())
