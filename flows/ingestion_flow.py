from prefect import flow, task, get_run_logger
from prefect.client.schemas.schedules import IntervalSchedule
from datetime import timedelta
from ingestion.scripts.raw_prices import extract_raw_prices
from ingestion.scripts.alternative_api import extract_fear_and_greed
from ingestion.scripts.subreddits_posts import extract_subreddits
from ingestion.scripts.fred_economic_data import extract_fred_api
from ingestion.scripts.coindesk_scraping import extract_coindesk_articles

     
@task
def run_ingestion():
    logger = get_run_logger()
    logger.info("Starting BTC Raw Price extraction")
    price_result = extract_raw_prices()
    fear_and_greed_result = extract_fear_and_greed()
    extract_subreddits_result =  extract_subreddits()
    extract_fred_api_result = extract_fred_api()
    extract_coindesk_articles_result = extract_coindesk_articles()
    
    logger.info(f"{price_result} - {fear_and_greed_result} - {extract_subreddits_result} - {extract_fred_api_result}- {extract_coindesk_articles_result}") 
    

        
    
@flow(name="ingestion_flow")
def ingestion_flow():
    run_ingestion()        
        
    
if __name__ == "__main__":    
    
    ingestion_flow.from_source(
        source=".",  # Directorio actual
        entrypoint="flows/ingestion_flow.py:ingestion_flow"
        ).deploy(
        name="daily_btc_ingestion_flow",
        work_pool_name='bit-scope_local_pool',
        schedule=IntervalSchedule(interval=timedelta(days=1)),
    )
    
    
    