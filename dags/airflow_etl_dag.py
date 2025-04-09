from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import sys
import os

# Add project root to sys.path for module imports
sys.path.append("/opt/airflow")

# Import your ETL script
from etl_pipeline import run_etl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data.engineer@agriaku.test"],
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="etl_course_attendance_pipeline",
    default_args=default_args,
    description="ETL pipeline for processing university course attendance data",
    schedule_interval=None,  # Or e.g., '0 2 * * *' for daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "attendance", "agriaku"]
) as dag:

    def run_etl_with_logging():
        try:
            logging.info("Starting ETL pipeline execution...")
            run_etl()
            logging.info("ETL pipeline executed successfully.")
        except Exception as e:
            logging.error(f"ETL pipeline failed: {e}")
            raise

    run_etl_task = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl_with_logging
    )

    run_etl_task
