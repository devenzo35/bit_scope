from prefect import Flow, Task, get_run_logger
from prefect.client.schemas.schedules import IntervalSchedule
from datetime import timedelta
from transformation.scripts.alternative_me.transform_fng import transform_fng
from transformation.scripts.fred_economic_data.transform_fred_economic_data import transform_fred_economic_data
from transformation.scripts.reddit_api.tansform_subreddits_posts import transform_subreddits_posts
from transformation.scripts.coindesk_scrap_articles.transform_coindesk_articles import transform_coindesk_articles
from transformation.scripts.coingecko_btc_price.transform_btc_prices import transform_btc_prices 

@Task
def run_transformation():
    logger = get_run_logger()
    logger.info("Starting Data Transformation")
    coingecko_result =  transform_btc_prices()
    logger.info(f"BTC Price Transformation result: {coingecko_result}")
    fng_result = transform_fng()
    logger.info(f"Fear and Greed Transformation result: {fng_result}")
    subreddits_result = transform_subreddits_posts()
    logger.info(f"Reddit Subreddits Transformation result: {subreddits_result}")
    fred_result = transform_fred_economic_data()
    logger.info(f"FRED Economic Data Transformation result: {fred_result}")
    coindesk_result = transform_coindesk_articles()
    logger.info(f"Coindesk Articles Transformation result: {coindesk_result}")
    logger.info("Data Transformation Completed. ✅")    

@Flow
def transform_flow():
    run_transformation()
    
if __name__ == "__main__":    
     transform_flow.from_source(
        source=".",  # Directorio actual
        entrypoint="flows/transform_flow.py:transform_flow"
    ).deploy(
        name="daily_data_transformation",
        work_pool_name='bit-scope_local_pool',
        schedule=IntervalSchedule(interval=timedelta(days=1)))