# -*- coding: utf-8 -*-
import pandas as pd
import datetime as dt
import numpy as np
from config.config import INGESTION_DATA_DIR,TRANSFORM_DATA_DIR, ALTERNATIVEME_ID as ID
from storage.repositories import FearAndGreedRepository

(TRANSFORM_DATA_DIR / ID).mkdir(parents=True, exist_ok=True)

def transform_fng():
    """
    Transforms the raw Fear & Greed index data and loads it into the database.
    """
    try:
        # Transformación
        df = pd.read_parquet(INGESTION_DATA_DIR / ID / "raw_fng_idx.parquet")

        df['timestamp'] = df['timestamp'].astype(int)
        df['date'] = pd.to_datetime(df['timestamp'], unit="s").dt.date
        df['value'] = df['value'].astype(int)

        df.rename(columns={'date': 'date', 'value': 'value', 'value_classification': 'value_classification'}, inplace=True)
        
        df_transformed = df[['date', 'value', 'value_classification']]
        df_transformed = df_transformed.drop_duplicates(subset=['date'])

        # Guardar en Parquet (opcional, pero buena práctica)
        df_transformed.to_parquet(TRANSFORM_DATA_DIR / ID / "transform_fng_idx.parquet", index=False)
        
        # Carga a la base de datos
        print("Loading transformed Fear & Greed data into the database...")
        repo = FearAndGreedRepository()
        repo.create_tables_from_schema()  # Asegura que la tabla exista
        repo.add_fng_data(df_transformed)
        
        return f"Transformed and loaded {len(df_transformed)} records for {ID} successfully."

    except Exception as e:
        return f"Error during transformation/loading for {ID}: {e}"
  

if __name__ == "__main__":
  transform_fng()