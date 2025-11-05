from pathlib import Path

ROOTDIR = Path(__file__).resolve().parents[1]

REDDIT_ID = "reddit_api"
FRED_ID = "fred_economic_data"
COINGECKO_ID = "coingecko_btc_price"
COINDESK_ID = "coindesk_articles"
ALTERNATIVEME_ID = "alternative_me"


TRANSFORM_DATA_DIR = ROOTDIR / "data" / "processed" 
INGESTION_DATA_DIR = ROOTDIR / "data" / "raw" 
