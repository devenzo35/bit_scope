-- schema.sql
-- Este esquema está diseñado para coincidir con la salida de los scripts de transformación originales.

-- De transformation/scripts/coingecko_btc_price/transform_btc_prices.py
CREATE TABLE IF NOT EXISTS btc_prices (
    date TIMESTAMP PRIMARY KEY,
    price REAL,
    market_cap REAL,
    total_volume REAL
);

-- De transformation/scripts/alternative_me/transform_fng.py
CREATE TABLE IF NOT EXISTS fear_and_greed (
    date TIMESTAMP PRIMARY KEY,
    value INTEGER,
    value_classification TEXT,
    hours_until_update REAL
);

-- De transformation/scripts/coindesk_scrap_articles/transform_coindesk_articles.py
CREATE TABLE IF NOT EXISTS coindesk_articles (
    title VARCHAR PRIMARY KEY,
    extra_info VARCHAR
);

-- De transformation/scripts/fred_economic_data/transform_fred_economic_data.py
CREATE TABLE IF NOT EXISTS cpi_data (
    date DATE PRIMARY KEY,
    monthly_inflation REAL
);

CREATE TABLE IF NOT EXISTS interest_rates_data (
    date DATE PRIMARY KEY,
    interest_rate REAL
);

CREATE TABLE IF NOT EXISTS spy_price_data (
    date DATE PRIMARY KEY,
    price REAL
);

CREATE TABLE IF NOT EXISTS unemployment_rate_data (
    date DATE PRIMARY KEY,
    unemp_rate REAL
);

-- De transformation/scripts/reddit_api/tansform_subreddits_posts.py
CREATE TABLE IF NOT EXISTS reddit_posts (
    id VARCHAR PRIMARY KEY,
    title VARCHAR,
    selftext VARCHAR,
    comments_count INTEGER,
    link_flair_text VARCHAR,
    permalink VARCHAR,
    score INTEGER,
    subreddit VARCHAR,
    upvote_ratio REAL,
    url VARCHAR,
    created_utc BIGINT
);
