"""
This script scrapes opinion articles from Coindesk, parses them,
filters out duplicates, and saves the data as a JSON file.
"""
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import json
from config.config import INGESTION_DATA_DIR, COINDESK_ID as ID

# --- Configuration ---
URL = "https://www.coindesk.com/opinion"
DATA_DIR = INGESTION_DATA_DIR / ID
FILE_PATH = DATA_DIR / "raw_coindesk_articles.json"

# Ensure the destination directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_soup(url: str) -> BeautifulSoup:
    """
    Fetches the HTML content from a URL and returns it as a BeautifulSoup object.

    Args:
        url: The URL to scrape.

    Returns:
        A BeautifulSoup object representing the parsed HTML.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; GeminiScraper/1.0; +http://google.com/bot)"
    }
    r = requests.get(url=url, headers=headers, timeout=15)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def parse_listing(soup: BeautifulSoup) -> dict[str, dict[str, str]]:
    """
    Parses the article listing page to extract article data.

    Args:
        soup: The BeautifulSoup object of the listing page.

    Returns:
        A dictionary of article data, with titles as keys.
    """
    rows: dict[str, dict[str, str]] = {}
    article_cards = soup.select("section div")

    for a in article_cards:
        title_el = a.select_one("a > h2")
        metadata_el = a.select_one("p")
        description_el = a.select_one("p > span")

        title = title_el.get_text(strip=True) if title_el else None
        metadata = metadata_el.get_text(strip=True) if metadata_el else None
        description = description_el.get_text(strip=True) if description_el else None

        if title and metadata and description:
            rows[title] = {
                "title": title,
                "metadata": metadata,
                "description": description,
            }
    return rows


def filter_duplicated_articles(articles: dict[str, dict[str, str]]) -> list[dict[str, str]]:
    """
    Filters a dictionary of articles to remove duplicates based on keys.

    Args:
        articles: A dictionary of articles, potentially with duplicates.

    Returns:
        A list of unique articles.
    """
    filter_rows: dict[str, dict[str, str]] = {}
    for k, v in articles.items():
        if filter_rows.get(k):
            pass
        else:
            filter_rows[k] = v
    return [article for article in filter_rows.values()]


def extract_coindesk_articles():
    """
    Main function to orchestrate the scraping, parsing, and saving of Coindesk articles.
    """
    print(f"Fetching {URL}...")
    try:
        soup = get_soup(URL)
        articles = parse_listing(soup)
        filtered_articles = filter_duplicated_articles(articles)

        data = {
            "metadata": {
                "source": "Coindesk",
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
            },
            "data": filtered_articles,
        }

        with open(FILE_PATH, "w", encoding="utf-8") as file:
            file.write(json.dumps(data, indent=4))
        
        print(f"Successfully saved {len(filtered_articles)} articles from {URL}")
        return f"{URL} OK"
    except requests.exceptions.RequestException as e:
        print(f"ERROR fetching data from {URL}: {e}")
        return f"{URL} ERROR"
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return f"{URL} ERROR"


if __name__ == "__main__":
    extract_coindesk_articles()
