from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from datetime import timedelta, datetime
import random
from .Daily_sales import download_data, validate_file, transform_data, load_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='daily_sales_pipeline',
    default_args=default_args,
    description='Automates daily sales ingestion to PostgreSQL',
    schedule_interval='0 6 * * *',
    catchup=False
)

start_pipeline = EmptyOperator(
    task_id='start_pipeline',
    dag=dag
)

download_task = PythonOperator(
    task_id='download_sales_data',
    python_callable=download_data,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_sales_file',
    python_callable=validate_file,
    dag=dag
)

def skip_if_empty(**kwargs):
    is_empty = kwargs['ti'].xcom_pull(task_ids='validate_sales_file', key='is_empty')
    return 'skip_load' if is_empty else 'transform_sales_data'

branch_task = BranchPythonOperator(
    task_id='branch_on_validation',
    python_callable=skip_if_empty,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_sales_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_sales_data_to_postgres',
    python_callable=load_data,
    dag=dag
)

skip_load = EmptyOperator(
    task_id='skip_load',
    dag=dag
)

success_notify = BashOperator(
    task_id='success_notification',
    bash_command='echo "Sales pipeline completed successfully!"',
    dag=dag
)


# Define dependencies
start_pipeline >> download_task >> validate_task >> branch_task
branch_task >> transform_task >> load_task >> success_notify
branch_task >> skip_load >> success_notify

