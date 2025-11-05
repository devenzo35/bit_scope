# -*- coding: utf-8 -*-
import pandas as pd
import datetime as dt
import numpy as np
from config.config import INGESTION_DATA_DIR,TRANSFORM_DATA_DIR, ALTERNATIVEME_ID as ID

(TRANSFORM_DATA_DIR / ID).mkdir(parents=True, exist_ok=True)

df = pd.read_parquet(INGESTION_DATA_DIR / ID / "raw_fng_idx.parquet")

df['timestamp'] = df['timestamp'].astype(int)
df['date'] = pd.to_datetime(df['timestamp'], unit="s")

df['time_until_update'] = pd.to_numeric(df['time_until_update'], errors="coerce")
df['hours_until_update'] = (df['time_until_update'] / 3600).round(2)

df.loc[df['time_until_update'] < 0, "time_until_update"] = np.nan

df.set_index('date', inplace=True)

df = df[['value', 'value_classification',
         #'timestamp', 'time_until_update',
       'hours_until_update']]

df.to_parquet(TRANSFORM_DATA_DIR / ID / "transform_fng_idx.parquet")