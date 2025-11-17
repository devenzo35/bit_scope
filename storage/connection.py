import duckdb
from config.config import STORAGE_DIR
from pathlib import Path

DB_FILE = STORAGE_DIR / "db" / "database.duckdb"
DB_FILE.parent.mkdir(parents=True, exist_ok=True)

def get_db_connection():
    """
    Returns a connection to the DuckDB database.
    """
    
    return duckdb.connect(DB_FILE)