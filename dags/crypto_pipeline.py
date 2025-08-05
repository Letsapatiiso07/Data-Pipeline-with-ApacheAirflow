from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import sys
import os

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
if scripts_path not in sys.path:
    sys.path.append(scripts_path)
try:
    from scripts.fetch_crypto_data import fetch_crypto_data, save_to_csv
except ImportError:
    raise ImportError(f"Could not import 'fetch_crypto_data'. Make sure 'fetch_crypto_data.py' exists in {scripts_path} and contains 'fetch_crypto_data' and 'save_to_csv'.")

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'crypto_data_pipeline',
    default_args=default_args,
    description='Daily pipeline to fetch and store crypto price data',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

start = DummyOperator(task_id='start', dag=dag)

def fetch_and_store():
    df = fetch_crypto_data()
    save_to_csv(df)

fetch_task = PythonOperator(
    task_id='fetch_and_store_crypto_data',
    python_callable=fetch_and_store,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> fetch_task >> end