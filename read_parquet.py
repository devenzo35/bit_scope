import pandas as pd
from config.config import STORAGE_DIR, TRANSFORM_DATA_DIR, FRED_ID, ALTERNATIVEME_ID

PARQUET_PATH = TRANSFORM_DATA_DIR / ALTERNATIVEME_ID / "transform_fng_idx.parquet"

pd.set_option('display.max_columns', 1000)
df = pd.read_parquet(PARQUET_PATH)
df.reset_index(inplace=True)
print(df.columns)