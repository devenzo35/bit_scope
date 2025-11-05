import pandas as pd
from fredapi import Fred  # type: ignore
from ingestion.logging.metadata_log import log_metadata
from pathlib import Path
from config.config import INGESTION_DATA_DIR, FRED_ID as ID

from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("FRED_API_KEY")
DESTINATION_DIR = INGESTION_DATA_DIR / ID

(DESTINATION_DIR).mkdir(parents=True, exist_ok=True)

def extract_fred_api():
    try:
        fred = Fred(api_key=API_KEY)

        interest_rates = fred.get_series("RIFSPFFNB")  # type: ignore
        df = pd.DataFrame(interest_rates)

        sp500 = fred.get_series("SP500")  # type: ignore
        df = pd.DataFrame(sp500)

        cpi = fred.get_series("CPIAUCSL")  # type: ignore
        cpi_df = pd.DataFrame(cpi)
        
        unem_rate = fred.get_series("UNRATE")  # type: ignore
        unem_rate.plot()

        unem_rate_df = pd.DataFrame(unem_rate)

        log_metadata(
        source="FRED API",
        file_path="unemp_rate.parquet, cpi_raw.parquet, sp500_series.parquet, interest_rates.parquet",
        rows=len(unem_rate_df),
        notes="Data fetched from FRED API, stored as parquet file, containing unemployment rates, interest rates, sp500 and consumer price index.",
    )
        
         
        df.to_parquet(DESTINATION_DIR / "raw_interest_rates.parquet")
        df.to_parquet(DESTINATION_DIR / "raw_sp500_series.parquet")
        cpi_df.to_parquet(DESTINATION_DIR / "raw_cpi.parquet")
        unem_rate_df.to_parquet(DESTINATION_DIR / "raw_unemp_rate.parquet")
        return f"{ID} OK"
        
    except:
        return f"{ID} ERROR"


if __name__ == "__main__":
    extract_fred_api()
