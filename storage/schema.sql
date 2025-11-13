-- schema.sql

-- Tabla para almacenar los precios de Bitcoin
CREATE TABLE IF NOT EXISTS btc_prices (
    date TIMESTAMP PRIMARY KEY,
    price REAL
);

-- Tabla para almacenar el índice de Miedo y Codicia (Fear and Greed)
CREATE TABLE IF NOT EXISTS fear_and_greed (
    date TIMESTAMP PRIMARY KEY,
    value INTEGER,
    value_classification TEXT
);

-- Tabla para almacenar los artículos de Coindesk
CREATE TABLE IF NOT EXISTS coindesk_articles (
    id VARCHAR PRIMARY KEY,
    url VARCHAR,
    title VARCHAR,
    description VARCHAR,
    author VARCHAR,
    author_url VARCHAR,
    publish_date TIMESTAMP,
    content VARCHAR
);

-- Tabla para almacenar los datos de CPI (Índice de Precios al Consumidor)
CREATE TABLE IF NOT EXISTS cpi_data (
    date DATE PRIMARY KEY,
    value REAL
);

-- Tabla para almacenar las tasas de interés
CREATE TABLE IF NOT EXISTS interest_rates_data (
    date DATE PRIMARY KEY,
    value REAL
);

-- Tabla para almacenar el precio del SPY
CREATE TABLE IF NOT EXISTS spy_price_data (
    date DATE PRIMARY KEY,
    value REAL
);

-- Tabla para almacenar la tasa de desempleo
CREATE TABLE IF NOT EXISTS unemployment_rate_data (
    date DATE PRIMARY KEY,
    value REAL
);

-- Tabla para almacenar los posts de Reddit
CREATE TABLE IF NOT EXISTS reddit_posts (
    id VARCHAR PRIMARY KEY,
    title VARCHAR,
    author VARCHAR,
    created_utc TIMESTAMP,
    score INTEGER,
    upvote_ratio REAL,
    full_link VARCHAR,
    num_comments INTEGER,
    num_crossposts INTEGER,
    total_awards_received INTEGER,
    selftext VARCHAR,
    subreddit VARCHAR,
    subreddit_subscribers INTEGER
);