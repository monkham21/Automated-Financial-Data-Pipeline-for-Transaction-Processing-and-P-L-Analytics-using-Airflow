from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import your modules (this is key for clean structure)
from src.extract.api_client import fetch_transactions
from src.transform.transform import transform_data
from src.load.load_to_db import load_raw_data, load_curated_data
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Default arguments
default_args = {
    "owner": "mon",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# DAG definition
with DAG(
    dag_id="finance_data_pipeline",
    default_args=default_args,
    description="Automated financial data pipeline with PnL transformation",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "etl"],
) as dag:

    # -------------------------
    # TASK 1: Extract
    # -------------------------
    def extract_task(**context):
        data = fetch_transactions()
        context["ti"].xcom_push(key="raw_data", value=data)
        logger.info("Extracted data successfully")

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_task,
    )

    # -------------------------
    # TASK 2: Load Raw
    # -------------------------
    def load_raw_task(**context):
        data = context["ti"].xcom_pull(key="raw_data", task_ids="extract_data")
        load_raw_data(data)
        logger.info("Loaded raw data")

    load_raw = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_task,
    )

    # -------------------------
    # TASK 3: Transform
    # -------------------------
    def transform_task(**context):
        data = context["ti"].xcom_pull(key="raw_data", task_ids="extract_data")
        transformed = transform_data(data)
        context["ti"].xcom_push(key="transformed_data", value=transformed)
        logger.info("Transformed data")

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task,
    )

    # -------------------------
    # TASK 4: Load Curated
    # -------------------------
    def load_curated_task(**context):
        data = context["ti"].xcom_pull(key="transformed_data", task_ids="transform_data")
        load_curated_data(data)
        logger.info("Loaded curated data")

    load_curated = PythonOperator(
        task_id="load_curated_data",
        python_callable=load_curated_task,
    )

    # -------------------------
    # TASK 5: Data Quality Check
    # -------------------------
    def data_quality_task():
        # simple check example
        logger.info("Running data quality checks...")
        # You can expand later
        return True

    data_quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_task,
    )

    # -------------------------
    # DAG Dependencies
    # -------------------------
    extract >> load_raw >> transform >> load_curated >> data_quality