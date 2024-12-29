from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from python_callable.kafka.portugal_listings_producer import producer_portugal_listings

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'catchup': True,
    'retries': 1,
    'schedule_interval': None,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG(
    'protugal_listings_consumer',
    default_args=default_args,
    description='Real Estate Streaming Consumer',
)

producer_task = PythonOperator(
    task_id='task_send_portugal_listings_to_kafka',
    python_callable=producer_portugal_listings,
    dag=dag
)

producer_task