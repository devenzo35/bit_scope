import pandas as pd
from storage.connection import get_db_connection

class BaseRepository:
    """
    Base class for all repositories.
    """
    def __init__(self):
        self.con = get_db_connection()

    def create_tables_from_schema(self, schema_file='storage/schema.sql'):
        """
        Creates tables based on a SQL schema file.
        This is a convenience method for setting up the database.
        """
        try:
            with open(schema_file, 'r') as f:
                sql = f.read()
                self.con.execute(sql)
            print("Tables checked/created successfully from schema.")
        except Exception as e:
            print(f"Error creating tables from schema: {e}")

class BtcPricesRepository(BaseRepository):
    """
    Repository for all database operations related to the btc_prices table.
    """
    def add_prices(self, prices_df: pd.DataFrame):
        """
        Adds new BTC prices to the database.
        """
        self.con.register('prices_df_view', prices_df)
        self.con.execute("""
            INSERT INTO btc_prices
            SELECT * FROM prices_df_view
            ON CONFLICT (date) DO UPDATE SET price = excluded.price
        """)
        print(f"{len(prices_df)} records added/updated in btc_prices.")

    def get_prices_in_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Retrieves BTC prices within a specific date range.
        """
        query = """
            SELECT * FROM btc_prices
            WHERE date >= ? AND date <= ?
        """
        return self.con.execute(query, [start_date, end_date]).fetchdf()

    def get_all_prices(self) -> pd.DataFrame:
        """
        Retrieves all BTC prices.
        """
        query = """
            SELECT * FROM btc_prices
        """
        return self.con.execute(query).fetchdf()
    
    def delete_all_prices(self):
        """
        Deletes all BTC prices from the database.
        """
        self.con.execute("DELETE FROM btc_prices")
        print("All records deleted from btc_prices.")

class FearAndGreedRepository(BaseRepository):
    """
    Repository for all database operations related to the fear_and_greed table.
    """
    def add_fng_data(self, fng_df: pd.DataFrame):
        """
        Adds new Fear and Greed data to the database.
        """
        self.con.register('fng_df_view', fng_df)
        self.con.execute("""
            INSERT INTO fear_and_greed
            SELECT * FROM fng_df_view
            ON CONFLICT (date) DO UPDATE SET
                value = excluded.value,
                value_classification = excluded.value_classification
        """)
        print(f"{len(fng_df)} records added/updated in fear_and_greed.")

    def get_fng_data_in_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Retrieves Fear and Greed data within a specific date range.
        """
        query = """
            SELECT * FROM fear_and_greed
            WHERE date >= ? AND date <= ?
        """
        return self.con.execute(query, [start_date, end_date]).fetchdf()

class CoindeskArticlesRepository(BaseRepository):
    """
    Repository for all database operations related to the coindesk_articles table.
    """
    def add_articles(self, articles_df: pd.DataFrame):
        """
        Adds new Coindesk articles to the database.
        """
        self.con.register('articles_df_view', articles_df)
        self.con.execute("""
            INSERT INTO coindesk_articles
            SELECT * FROM articles_df_view
            ON CONFLICT (id) DO UPDATE SET
                url = excluded.url,
                title = excluded.title,
                description = excluded.description,
                author = excluded.author,
                author_url = excluded.author_url,
                publish_date = excluded.publish_date,
                content = excluded.content
        """)
        print(f"{len(articles_df)} records added/updated in coindesk_articles.")

    def get_articles_in_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Retrieves Coindesk articles within a specific date range.
        """
        query = """
            SELECT * FROM coindesk_articles
            WHERE date >= ? AND date <= ?
        """
        return self.con.execute(query, [start_date, end_date]).fetchdf()

class FredEconomicDataRepository(BaseRepository):
    """
    Repository for all database operations related to FRED economic data tables.
    """
    def add_fred_data(self, fred_df: pd.DataFrame, table_name: str):
        """
        Adds new FRED economic data to the specified table.
        """
        # Ensure table_name is one of the allowed values to prevent SQL injection
        allowed_tables = ['cpi_data', 'interest_rates_data', 'spy_price_data', 'unemployment_rate_data']
        if table_name not in allowed_tables:
            raise ValueError(f"Invalid table name: {table_name}")

        self.con.register('fred_df_view', fred_df)
        self.con.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM fred_df_view
            ON CONFLICT (date) DO UPDATE SET value = excluded.value
        """)
        print(f"{len(fred_df)} records added/updated in {table_name}.")

    def get_fred_data_in_range(self, table_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Retrieves FRED economic data from the specified table within a specific date range.
        """
        query = f"""
            SELECT * FROM {table_name}
            WHERE date >= ? AND date <= ?
        """
        return self.con.execute(query, [start_date, end_date]).fetchdf()

class RedditPostsRepository(BaseRepository):
    """
    Repository for all database operations related to the reddit_posts table.
    """
    def add_posts(self, posts_df: pd.DataFrame):
        """
        Adds new Reddit posts to the database.
        """
        self.con.register('posts_df_view', posts_df)
        self.con.execute("""
            INSERT INTO reddit_posts
            SELECT * FROM posts_df_view
            ON CONFLICT (id) DO UPDATE SET
                title = excluded.title,
                author = excluded.author,
                created_utc = excluded.created_utc,
                score = excluded.score,
                upvote_ratio = excluded.upvote_ratio,
                full_link = excluded.full_link,
                num_comments = excluded.num_comments,
                num_crossposts = excluded.num_crossposts,
                total_awards_received = excluded.total_awards_received,
                selftext = excluded.selftext,
                subreddit = excluded.subreddit,
                subreddit_subscribers = excluded.subreddit_subscribers
        """)
        print(f"{len(posts_df)} records added/updated in reddit_posts.")

    def get_posts_in_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Retrieves Reddit posts within a specific date range.
        """
        query = """
            SELECT * FROM reddit_posts
            WHERE date >= ? AND date <= ?
        """
        return self.con.execute(query, [start_date, end_date]).fetchdf()
