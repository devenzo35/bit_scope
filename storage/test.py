from storage.repositories import BtcPricesRepository
import pandas as pd
from config.config import TRANSFORM_DATA_DIR, COINGECKO_ID

PRICES_PARQUET_PATH = TRANSFORM_DATA_DIR / COINGECKO_ID / "btc_prices.parquet"

def test_add_and_get_prices():
    
    prices_df = pd.read_parquet(PRICES_PARQUET_PATH)
    prices_df.reset_index(inplace=True)
    
    repo = BtcPricesRepository()
    # Create tables from schema
    repo.create_tables_from_schema()

    # Add prices
    repo.add_prices(prices_df)

    # Retrieve prices
    retrieved_df = repo.get_all_prices()
    
    print(retrieved_df.head())
    print("Test completed successfully.")

    # assert len(retrieved_df) == 2
    # assert retrieved_df.iloc[0]['price'] == 30000.0
    # assert retrieved_df.iloc[1]['price'] == 31000.0
    
test_add_and_get_prices()