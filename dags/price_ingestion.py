from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from ingestion.scripts.raw_prices import main as run_raw_prices

default_args: dict[str, str | int | timedelta] = {
    "owner": "airflow",
    "retries": 1,  # Número de reintentos
    "retry_delay": timedelta(minutes=1),  # Tiempo entre reintentos
    "execution_timeout": timedelta(minutes=10),  # Timeout máximo
}

with DAG(
    dag_id="btc_price_ingestion",
    default_args=default_args,
    start_date=datetime(2025, 10, 29),
    schedule="@daily",
    catchup=False,
):
    task1 = PythonOperator(task_id="btc_price", python_callable=run_raw_prices)
