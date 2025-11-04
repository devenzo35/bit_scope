import asyncio
import asyncpraw
from datetime import datetime, timezone
import json
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()
# from google.colab import userdata
ID = "REDDIT API"
ROOTDIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOTDIR / "data" / "raw" / ID
DATA_DIR.mkdir(parents=True, exist_ok=True)

def safe_attr(obj, attr):
    return getattr(obj, attr, None)


def extract_submission_data(submission) -> dict[str, str]:
    """
    Extract all relevant fields from a PRAW submission object
    and return as a Python dictionary.
    """

    return {
        "id": submission.id,
        "name": submission.name,
        "title": submission.title,
        "selftext": submission.selftext,
        "author": str(submission.author) if submission.author else None,
        "author_flair_text": submission.author_flair_text,
        "clicked": submission.clicked,
        "comments_count": submission.num_comments,
        "distinguished": submission.distinguished,
        "edited": submission.edited,
        "is_original_content": submission.is_original_content,
        "is_self": submission.is_self,
        "link_flair_template_id": safe_attr(submission, "link_flair_template_id"),
        "link_flair_text": submission.link_flair_text,
        "locked": submission.locked,
        "over_18": submission.over_18,
        "permalink": submission.permalink,
        # "poll_data": safe_attr(submission, 'poll_data'),
        "saved": submission.saved,
        "score": submission.score,
        "spoiler": submission.spoiler,
        "stickied": submission.stickied,
        "subreddit": str(submission.subreddit) if submission.subreddit else None,
        "upvote_ratio": submission.upvote_ratio,
        "url": submission.url,
        "created_utc": submission.created_utc,
    }


async def extract_subreddits():
    
    try:
        reddit = asyncpraw.Reddit(
        client_id=os.getenv("REDDIS_CLIENT_ID"),
        client_secret=os.getenv("REDDIS_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIS_USER_AGENT"),
        )

        subreddits = await reddit.subreddit("Bitcoin+btc+CryptoCurrency")

        async def get_submission_data():
            posts: list[dict[str, str]] = []

            async for submission in subreddits.new(limit=1000):
                posts.append(extract_submission_data(submission))

            return posts

        submission_data = await get_submission_data()  # type: ignore

        data: dict[str, list[dict[str, str]] | dict[str, str]] = {
        "metadata": {
            "source": "CoinGecko",
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0",
        },
        "data": submission_data,
    }
        with open(DATA_DIR / 'raw_subreddits_posts.json', 'w') as file:
            file.write(json.dumps(data, indent=4))
        return f"{ID} OK"
    except:
        return f"{ID} ERROR"


if __name__ == "__main__":
    asyncio.run(extract_subreddits())
