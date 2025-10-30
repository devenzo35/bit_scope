import pandas as pd
from fredapi import Fred  # type: ignore
from ingestion.logging.metadata_log import log_metadata

from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("FRED_API_KEY")


def main():
    fred = Fred(api_key=API_KEY)

    interest_rates = fred.get_series("RIFSPFFNB")  # type: ignore

    df = pd.DataFrame(interest_rates)
    df.to_parquet("interest_rates.parquet")

    sp500 = fred.get_series("SP500")  # type: ignore
    df = pd.DataFrame(sp500)
    df.to_parquet("sp500_series.parquet")

    cpi = fred.get_series("CPIAUCSL")  # type: ignore

    cpi_df = pd.DataFrame(cpi)
    cpi_df.to_parquet("cpi_raw.parquet")

    unem_rate = fred.get_series("UNRATE")  # type: ignore
    unem_rate.plot()

    unem_rate_df = pd.DataFrame(unem_rate)

    log_metadata(
        source="FRED API",
        file_path="unemp_rate.parquet, cpi_raw.parquet, sp500_series.parquet, interest_rates.parquet",
        rows=len(unem_rate_df),
        notes="Data fetched from FRED API, stored as parquet file, containing unemployment rates, interest rates, sp500 and consumer price index.",
    )

    unem_rate_df.to_parquet("unemp_rate.parquet")


if __name__ == "__main__":
    main()
