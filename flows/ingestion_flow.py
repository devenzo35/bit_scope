from prefect import flow, task, get_run_logger
from prefect.client.schemas.schedules import IntervalSchedule
from datetime import timedelta
from ingestion.scripts.raw_prices import extract_raw_prices
from ingestion.scripts.alternative_api import extract_fear_and_greed
from ingestion.scripts.subreddits_posts import extract_subreddits
from ingestion.scripts.fred_economic_data import extract_fred_api
from ingestion.scripts.coindesk_scraping import extract_coindesk_articles
import asyncio

     
@task
async def run_ingestion():
    logger = get_run_logger()
    logger.info("Starting BTC Raw Price extraction")
    coingecko_data_result = extract_raw_prices()
    logger.info(f"BTC Raw Price extraction result: {coingecko_data_result}")
    fear_and_greed_result = extract_fear_and_greed()
    logger.info(f"Fear and Greed extraction result: {fear_and_greed_result}")
    extract_subreddits_result =  await extract_subreddits()
    logger.info(f"Reddit Subreddits extraction result: {extract_subreddits_result}")
    extract_fred_api_result = extract_fred_api()
    logger.info(f"FRED API extraction result: {extract_fred_api_result}")
    extract_coindesk_articles_result = extract_coindesk_articles()
    logger.info(f"Coindesk Articles extraction result: {extract_coindesk_articles_result}")
    
    logger.info("Ingestion flow completed. ✅")

    
        
    

        
    
@flow(name="ingestion_flow")
async def ingestion_flow():
   await run_ingestion()        
        
    
if __name__ == "__main__":    
    
   asyncio.run( ingestion_flow.from_source(
        source=".",  # Directorio actual
        entrypoint="flows/ingestion_flow.py:ingestion_flow"
        ).deploy(
        name="daily_data_ingestion",
        work_pool_name='bit-scope_local_pool',
        schedule=IntervalSchedule(interval=timedelta(days=1)),
    ))
    
    
    