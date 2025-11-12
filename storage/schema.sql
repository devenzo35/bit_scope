CREATE TABLE IF NOT EXISTS btc_prices (
    date TIMESTAMP,
    price DOUBLE
);

CREATE TABLE IF NOT EXISTS fear_and_greed (
    date TIMESTAMP,
    value INTEGER,
    classification VARCHAR
);

CREATE TABLE IF NOT EXISTS coindesk_articles (
    date TIMESTAMP,
    title VARCHAR,
    url VARCHAR,
    content VARCHAR
);

CREATE TABLE IF NOT EXISTS cpi (
    date DATE,
    value DOUBLE
);

CREATE TABLE IF NOT EXISTS interest_rates (
    date DATE,
    value DOUBLE
);

CREATE TABLE IF NOT EXISTS spy_price (
    date DATE,
    value DOUBLE
);

CREATE TABLE IF NOT EXISTS unemployment_rate (
    date DATE,
    value DOUBLE
);

CREATE TABLE IF NOT EXISTS reddit_posts (
    date TIMESTAMP,
    subreddit VARCHAR,
    title VARCHAR,
    content VARCHAR,
    url VARCHAR
);
