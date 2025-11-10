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

def transform_coindesk_articles():

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    with raw_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Manejar estructuras con o sin clave 'data'
    records = data.get("data", data)

    df = pd.DataFrame(records)

    # Seleccionar y renombrar columnas
    if "title" in df.columns and "metadata" in df.columns:
        new_df = df[["title", "metadata"]].copy()
        new_df = new_df.rename(columns={"metadata": "extra_info"})
    else:
        # Si la estructura es distinta, crear df vacio con las columnas esperadas
        new_df = pd.DataFrame(columns=["title", "extra_info"])

    # Asegurar dtypes
    new_df["title"] = new_df["title"].astype("string")
    new_df["extra_info"] = new_df["extra_info"].astype("string")

    # Guardar resultado procesado
    new_df.to_parquet(out_file, index=False)
    print(f"Processed file saved in: {out_file}")
    
    return f"{ID} OK."
    
if __name__ == "__main__":
    transform_coindesk_articles()