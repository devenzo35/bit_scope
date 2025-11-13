# ...existing code...
import json
from pathlib import Path
import pandas as pd

# Importar configuración de rutas
from config.config import INGESTION_DATA_DIR, TRANSFORM_DATA_DIR, COINDESK_ID as ID

# Rutas basadas en config
raw_path = INGESTION_DATA_DIR / ID / "raw_coindesk_articles.json"
out_dir = TRANSFORM_DATA_DIR / ID
out_dir.mkdir(parents=True, exist_ok=True)
out_file = out_dir / "coindesk_articles.parquet"

from storage.repositories import CoindeskArticlesRepository

def transform_coindesk_articles():
    """
    Transforms the raw Coindesk articles data and loads it into the database.
    """
    try:
        if not raw_path.exists():
            raise FileNotFoundError(f"Raw file not found: {raw_path}")

        with raw_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        # Transformación
        articles = [item['attributes'] for item in data['data']]
        df = pd.DataFrame(articles)

        df_transformed = df[['url', 'title', 'description', 'author', 'author_url', 'publish_date', 'content']].copy()
        df_transformed['id'] = df_transformed['url'].apply(lambda x: x.split('/')[-2])
        df_transformed['publish_date'] = pd.to_datetime(df_transformed['publish_date'])
        
        # Reordenar para que coincida con el schema.sql
        df_transformed = df_transformed[['id', 'url', 'title', 'description', 'author', 'author_url', 'publish_date', 'content']]

        # Guardar en Parquet
        df_transformed.to_parquet(out_file, index=False)
        print(f"Processed file saved in: {out_file}")

        # Carga a la base de datos
        print("Loading transformed Coindesk articles into the database...")
        repo = CoindeskArticlesRepository()
        repo.create_tables_from_schema()
        repo.add_articles(df_transformed)

        return f"Transformed and loaded {len(df_transformed)} records for {ID} successfully."

    except Exception as e:
        return f"Error during transformation/loading for {ID}: {e}"
    
if __name__ == "__main__":
    transform_coindesk_articles()