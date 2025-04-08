# dags/airflow_etl_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
import os

# Add path to import your pipeline
sys.path.append(os.path.join(os.path.dirname(__file__)))
from etl_pipeline import run_etl

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='weekly_attendance_etl',
    default_args=default_args,
    schedule_interval='@daily',  # or '@once' for testing
    catchup=False
) as dag:

    etl_task = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_etl
    )

    etl_task
