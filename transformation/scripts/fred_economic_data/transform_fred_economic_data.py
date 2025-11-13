import pandas as pd
import datetime as dt
from config.config import INGESTION_DATA_DIR,TRANSFORM_DATA_DIR, FRED_ID as ID

SOURCE_DIR = INGESTION_DATA_DIR / ID
DEST_DIR = TRANSFORM_DATA_DIR / ID
(DEST_DIR).mkdir(parents=True, exist_ok=True)

from storage.repositories import FredEconomicDataRepository

def transform_fred_economic_data():
    """
    Transforms multiple FRED economic datasets and loads them into their respective tables in the database.
    """
    try:
        # --- Transformación ---
        
        # 1. CPI
        cpi_df = pd.read_parquet(SOURCE_DIR / 'raw_cpi.parquet')
        cpi_df.rename(columns={cpi_df.columns[0]: 'value'}, inplace=True)
        cpi_df['value'] = (cpi_df['value'].pct_change(periods=12) * 100).round(2)
        cpi_df.dropna(inplace=True)
        cpi_df.index.name = 'date'
        cpi_df.to_parquet(DEST_DIR / 'transform_cpi.parquet')

        # 2. Interest Rates
        ir_df = pd.read_parquet(SOURCE_DIR / 'raw_interest_rates.parquet')
        ir_df.rename(columns={ir_df.columns[0]: 'value'}, inplace=True)
        ir_df.index.name = 'date'
        ir_df.to_parquet(DEST_DIR / 'transform_interest_rates.parquet')

        # 3. SPY Price
        spy_df = pd.read_parquet(SOURCE_DIR / 'raw_sp500_series.parquet')
        spy_df.rename(columns={spy_df.columns[0]: 'value'}, inplace=True)
        spy_df.index.name = 'date'
        spy_df.to_parquet(DEST_DIR / 'transform_spy_price.parquet')

        # 4. Unemployment Rate
        unemp_df = pd.read_parquet(SOURCE_DIR / 'raw_unemp_rate.parquet')
        unemp_df.rename(columns={unemp_df.columns[0]: 'value'}, inplace=True)
        unemp_df.index.name = 'date'
        unemp_df.to_parquet(DEST_DIR / 'transform_unemp_rate.parquet')

        # --- Carga a la base de datos ---
        print("Loading transformed FRED data into the database...")
        repo = FredEconomicDataRepository()
        repo.create_tables_from_schema()

        # Crear un diccionario para iterar y guardar cada dataset
        datasets = {
            "cpi_data": cpi_df.reset_index(),
            "interest_rates_data": ir_df.reset_index(),
            "spy_price_data": spy_df.reset_index(),
            "unemployment_rate_data": unemp_df.reset_index()
        }

        for table_name, df in datasets.items():
            repo.add_fred_data(df, table_name)
            print(f"Loaded {len(df)} records into {table_name}.")

        return f"Transformed and loaded all datasets for {ID} successfully."

    except Exception as e:
        return f"Error during transformation/loading for {ID}: {e}"
    
    
if __name__ == "__main__":
    transform_fred_economic_data()