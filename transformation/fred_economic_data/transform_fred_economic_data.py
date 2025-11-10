import pandas as pd
import datetime as dt
from config.config import INGESTION_DATA_DIR,TRANSFORM_DATA_DIR, FRED_ID as ID

SOURCE_DIR = INGESTION_DATA_DIR / ID
DEST_DIR = TRANSFORM_DATA_DIR / ID
(DEST_DIR).mkdir(parents=True, exist_ok=True)

cpi_df = pd.read_parquet(SOURCE_DIR / 'raw_cpi.parquet')
ir_df = pd.read_parquet(SOURCE_DIR / 'raw_interest_rates.parquet')
spy_df = pd.read_parquet(SOURCE_DIR / 'raw_sp500_series.parquet')
unemp_df = pd.read_parquet(SOURCE_DIR / 'raw_unemp_rate.parquet')

cpi_df['monthly_inflation'] = (cpi_df[0].pct_change(periods=12) * 100).round(2)

get_today = dt.date.today()
cpi_df = cpi_df.loc["2000-01-01": f"{get_today.year}-{get_today.month}-01" ]

cpi_df.index.name = 'date'
cpi_df.drop(columns=0, inplace=True)





ir_df.index.name = 'date'
ir_df.rename(columns={0:"interest_rate"}, inplace=True)



spy_df.index.name = 'date'
spy_df.rename(columns={0:'price'}, inplace=True)



unemp_df.index.name = 'date'
unemp_df.rename(columns={0:'unemp_rate'}, inplace=True)



cpi_df.to_parquet(DEST_DIR / 'transform_cpi.parquet')
ir_df.to_parquet(DEST_DIR / 'transform_interest_rates.parquet')
spy_df.to_parquet(DEST_DIR / 'transform_spy_price.parquet')
unemp_df.to_parquet(DEST_DIR / 'transform_unemp_rate.parquet')