from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import json


soup = BeautifulSoup()

URL = "https://www.coindesk.com/opinion"
OUTDIR = Path("snapshots")
OUTDIR.mkdir(exist_ok=True)


def get_soup(url: str):
    """

    Args:
        url (str): _site url from where the content is to be scraped_

    Returns:
        BeautifulSoup: _Parsed HTML content_
    """

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; EnScraper/1.0; +https://example.com/bot)"
    }

    r = requests.get(url=url, headers=headers, timeout=15)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def parse_listing(soup: BeautifulSoup) -> dict[str, dict[str, str]]:
    """

    Args:
        soup (BeautifulSoup): _Soup object of the listing page_

    Returns:
        dict[str, dict[str, str]]: _Parsed article data_
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


def filter_duplicated_articles(articles: dict[str, dict[str, str]]):
    """filter articles because of duplication, and save to json file

    Args:
        articles (dict[str, dict[str, str]]): _duplicated article data_
    Returns:
        None

    """

    filter_rows: dict[str, dict[str, str]] = {}

    for k, v in articles.items():
        if filter_rows.get(k):
            pass
        else:
            filter_rows[k] = v

    filtered_articles = [article for article in filter_rows.values()]
    return filtered_articles


def main():
    print(f"fetching {URL}...")
    soup = get_soup(URL)
    articles = parse_listing(soup)
    filtered_articles = filter_duplicated_articles(articles)

    data: dict[str, list[dict[str, str]] | dict[str, str]] = {
        "metadata": {
            "source": "CoinGecko",
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0",
        },
        "data": filtered_articles,
    }

    with open("articles.json", "w") as file:
        file.write(json.dumps(data, indent=4))


if __name__ == "__main__":
    main()
