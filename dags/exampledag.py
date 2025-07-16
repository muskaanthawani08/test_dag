from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='simple_dag',
    start_date=datetime(2025, 7, 16),
    schedule_interval='@daily',
    catchup=False,
    description='A simple DAG with two dummy tasks'
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    end_task = DummyOperator(
        task_id='end'
    )

    # Set task dependencies
    start_task >> end_task

