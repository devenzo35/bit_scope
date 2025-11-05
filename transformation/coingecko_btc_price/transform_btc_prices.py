import pandas as pd
from pathlib import Path
from config.config import INGESTION_DATA_DIR as INGESTION_DIR
from config.config import TRANSFORM_DATA_DIR as TRANSFORM_DIR
from config.config import COINGECKO_ID as ID

(TRANSFORM_DIR / ID).mkdir(parents=True, exist_ok=True)

df = pd.read_parquet(INGESTION_DIR / ID / "raw_btc_prices.parquet")

df["date"] = [elem[0] for elem in df["prices"]]
df["date"] = pd.to_datetime(df["date"], unit="ms")

df.set_index("date", inplace=True)

df["prices"] = [round(elem[1], 2) for elem in df["prices"]]
df["market_caps"] = [round(elem[1], 2) for elem in df["market_caps"]]
df["total_volumes"] = [round(elem[1], 2) for elem in df["total_volumes"]]

df.rename(columns={"prices":"price",  "market_caps":"market_cap", "total_volumes":"total_volume" }, inplace=True)

df = df.dropna(subset=['price'])
df = df.dropna(subset=['market_cap'])
df = df.dropna(subset=['total_volume'])

assert (df['price'] > 0).all(), "Found positive prices"

is_empty = df.loc[df['market_cap'].duplicated()]
is_empty.empty

df.dtypes

df.describe()

df.to_parquet(TRANSFORM_DIR / ID /  "btc_prices.parquet")