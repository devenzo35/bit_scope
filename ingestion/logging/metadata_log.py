import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def init_metadata_db(db_path: str = "ingestion/logging/data/metadata.db"):
    Path("ingestion/logging/data").mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            file_path TEXT,
            retrieved_at TEXT,
            rows INTEGER,
            version TEXT,
            notes TEXT
        )
    """)
    conn.commit()
    conn.close()


init_metadata_db()


def log_metadata(
    source: str,
    file_path: str,
    rows: int,
    version: str = "1.0",
    notes: str = "",
    db_path: str = "ingestion/logging/data/metadata.db",
):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO metadata (source, file_path, retrieved_at, rows, version, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (
            source,
            file_path,
            datetime.now(timezone.utc).isoformat(),
            rows,
            version,
            notes,
        ),
    )
    conn.commit()
    conn.close()
