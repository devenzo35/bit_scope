import pandas as pd
import sqlite3

conn = sqlite3.connect("ingestion/logging/data/metadata.db")


df = pd.read_sql_query("SELECT * FROM metadata", conn)
print(df)
conn.close()
