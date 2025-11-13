import pandas as pd
from pathlib import Path
from config.config import INGESTION_DATA_DIR as INGESTION_DIR
from config.config import TRANSFORM_DATA_DIR as TRANSFORM_DIR
from config.config import COINGECKO_ID as ID

(TRANSFORM_DIR / ID).mkdir(parents=True, exist_ok=True)

from storage.repositories import BtcPricesRepository

def transform_btc_prices():
    """
    Transforms the raw BTC price data and loads it into the database.
    """
    try:
        # Transformación
        df = pd.read_parquet(INGESTION_DIR / ID / "raw_btc_prices.parquet")

        df["date"] = pd.to_datetime([elem[0] for elem in df["prices"]], unit="ms")
        df["price"] = [round(elem[1], 2) for elem in df["prices"]]
        
        df_transformed = df[['date', 'price']].copy()
        df_transformed = df_transformed.dropna(subset=['price'])
        df_transformed = df_transformed.drop_duplicates(subset=['date'])

        assert (df_transformed['price'] > 0).all(), "Found non-positive prices"

        # Guardar en Parquet
        df_transformed.to_parquet(TRANSFORM_DIR / ID / "btc_prices.parquet", index=False)

        # Carga a la base de datos
        print("Loading transformed BTC prices into the database...")
        repo = BtcPricesRepository()
        repo.create_tables_from_schema()
        repo.add_prices(df_transformed)

        return f"Transformed and loaded {len(df_transformed)} records for {ID} successfully."

    except Exception as e:
        return f"Error during transformation/loading for {ID}: {e}"
    
if __name__ == "__main__":
    transform_btc_prices()