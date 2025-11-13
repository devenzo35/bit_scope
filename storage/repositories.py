import pandas as pd
from storage.connection import get_db_connection

class BaseRepository:
    """
    Clase base para todos los repositorios.
    """
    def __init__(self):
        self.con = get_db_connection()

    def create_tables_from_schema(self, schema_file='storage/schema.sql'):
        """
        Crea tablas basadas en un archivo de esquema SQL.
        """
        try:
            with open(schema_file, 'r') as f:
                sql = f.read()
                self.con.execute(sql)
            print("Tables checked/created successfully from schema.")
        except Exception as e:
            print(f"Error creating tables from schema: {e}")

    def _execute_upsert(self, df: pd.DataFrame, table_name: str, pk_column: str):
        """Método de ayuda para realizar inserciones/actualizaciones."""
        temp_view = f"{table_name}_view"
        self.con.register(temp_view, df)
        
        # Construye la parte SET de la consulta dinámicamente
        set_clauses = ", ".join([f'"{col}" = excluded."{col}"' for col in df.columns if col != pk_column])
        
        query = f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_view}
            ON CONFLICT ({pk_column}) DO UPDATE SET {set_clauses}
        """
        self.con.execute(query)
        print(f"{len(df)} records added/updated in {table_name}.")


class BtcPricesRepository(BaseRepository):
    def add_data(self, df: pd.DataFrame):
        self._execute_upsert(df, 'btc_prices', 'date')

class FearAndGreedRepository(BaseRepository):
    def add_data(self, df: pd.DataFrame):
        self._execute_upsert(df, 'fear_and_greed', 'date')

class CoindeskArticlesRepository(BaseRepository):
    def add_data(self, df: pd.DataFrame):
        # El DataFrame original no tiene PK, usamos 'title' como tal.
        self._execute_upsert(df, 'coindesk_articles', 'title')

class FredEconomicDataRepository(BaseRepository):
    def add_cpi_data(self, df: pd.DataFrame):
        self._execute_upsert(df, 'cpi_data', 'date')

    def add_interest_rates_data(self, df: pd.DataFrame):
        self._execute_upsert(df, 'interest_rates_data', 'date')

    def add_spy_price_data(self, df: pd.DataFrame):
        self._execute_upsert(df, 'spy_price_data', 'date')

    def add_unemployment_rate_data(self, df: pd.DataFrame):
        self._execute_upsert(df, 'unemployment_rate_data', 'date')

class RedditPostsRepository(BaseRepository):
    def add_data(self, df: pd.DataFrame):
        self._execute_upsert(df, 'reddit_posts', 'id')