import sys
sys.path.append("/opt/airflow")

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract.api_client import fetch_transactions
from src.transform.transform import calculate_metrics
from src.load.db import create_tables, insert_transactions, insert_metrics


def extract_task(**context):
    data = fetch_transactions()
    context["ti"].xcom_push(key="data", value=data)


def load_task(**context):
    data = context["ti"].xcom_pull(key="data", task_ids="extract")

    if not data:
        raise ValueError("No data received from extract task")

    print(f"Inserting {len(data)} records into exchange_rates")

    insert_transactions(data)

    print("Insert successful")


def transform_task(**context):
    data = context["ti"].xcom_pull(key="data", task_ids="extract")

    if not data:
        raise ValueError("No data for transform")

    print(f"Transform received {len(data)} records")

    metrics = calculate_metrics(data)

    print(f"Calculated metrics: {metrics}")

    context["ti"].xcom_push(key="metrics", value=metrics)


def load_metrics_task(**context):
    metrics = context["ti"].xcom_pull(key="metrics", task_ids="transform")

    if not metrics:
        raise ValueError("No metrics to load")

    print(f"Inserting metrics: {metrics}")

    insert_metrics(metrics)

    print("Metrics insert successful")


def setup_db():
    create_tables()


with DAG(
    dag_id="finance_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    setup = PythonOperator(task_id="setup_db", python_callable=setup_db)

    extract = PythonOperator(task_id="extract", python_callable=extract_task)

    load = PythonOperator(task_id="load_transactions", python_callable=load_task)

    transform = PythonOperator(task_id="transform", python_callable=transform_task)

    load_metrics = PythonOperator(
        task_id="load_metrics",
        python_callable=load_metrics_task,
    )

    setup >> extract >> load >> transform >> load_metrics