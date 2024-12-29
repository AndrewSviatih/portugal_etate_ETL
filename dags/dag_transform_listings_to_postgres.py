from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from python_callable.transform_listings_to_postgres import transform_listings_to_postgres

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
    'agg_portugal_listings',
    default_args=default_args,
    description='Agg Portugues Listings',
)

producer_task = PythonOperator(
    task_id='task_agg_portugal_listings',
    python_callable=transform_listings_to_postgres,
    dag=dag
)

producer_task