from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from datetime import timedelta, datetime
import os
import math
import pandas as pd
import logging
import mysql.connector
from dotenv import load_dotenv
import snowflake.connector as sf

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def download_data(**kwargs):
    try:
        conn = sf.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )

        cur = conn.cursor()

        query = "SELECT * FROM daily_sales"
        cur.execute(query)
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]
        df = pd.DataFrame(rows, columns=columns)

        logging.info("data preview:\n%s", df.head().to_string(index=False))  

        kwargs['ti'].xcom_push(key='file_path', value=df)

        logging.info(f"Fetched {len(df)} rows from Snowflake.")

    except Exception as e:
        logging.error(f"Snowflake error: {e}")
        raise

    finally:
        cur.close()
        conn.close()


def validate_file(**kwargs):
    try:
        df = kwargs['ti'].xcom_pull(key='file_path')

        if df is None:
            logging.warning("No DataFrame found; treating as empty.")
            kwargs['ti'].xcom_push(key='is_empty', value=True)
            return

        is_empty = df.empty
        kwargs['ti'].xcom_push(key='is_empty', value=is_empty)
        logging.info(f"File validation complete: Empty={is_empty}")
    except Exception as e:
        logging.error(f"Error in validate_file: {e}")
        raise


def transform_data(**kwargs):
    try:
        df = kwargs['ti'].xcom_pull(key='file_path')

        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Time'] = pd.to_datetime(df['Time'], format='%H:%M', errors='coerce').dt.time

        df.dropna(inplace=True)
        df.rename(columns={'Invoice ID': 'Invoice_id'}, inplace=True)

        df['bracket'] = df['cogs'].apply(lambda cogs: f"{math.floor(cogs / 50) * 50}-{(math.floor(cogs / 50) + 1) * 50}")

        kwargs['ti'].xcom_push(key='cleaned_path', value=df)
        logging.info("Data transformed.")
    except Exception as e:
        logging.error(f"Error in transform_data: {e}")
        raise


def load_data(**kwargs):
    df = kwargs['ti'].xcom_pull(key='cleaned_path')

    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=int(os.getenv("DB_PORT", "3306"))
        )
        cur = conn.cursor()

        insert_query = """
            INSERT INTO daily_sales (
                Invoice_id, Store, City, Customer_type, Gender, Product_line,
                Unit_price, Quantity, Tax_5_percent, Total, Date, Time, Payment,
                cogs, gross_margin_percentage, gross_income, Rating, bracket
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data = [
            (
                row['Invoice_id'], row['Store'], row['City'], row['Customer type'], row['Gender'],
                row['Product line'], row['Unit price'], row['Quantity'], row['Tax 5%'], row['Total'],
                row['Date'], row['Time'], row['Payment'], row['cogs'], row['gross margin percentage'],
                row['gross income'], row['Rating'], row['bracket']
            )
            for _, row in df.iterrows()
        ]

        cur.executemany(insert_query, data)
        conn.commit()
        logging.info("Data loaded to MySQL.")

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    finally:
        if conn.is_connected():
            cur.close()
            conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='daily_sales_pipeline',
    default_args=default_args,
    description='Automates daily sales ingestion to PostgreSQL',
    schedule='0 6 * * *',
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
    try:
        is_empty = kwargs['ti'].xcom_pull(task_ids='validate_sales_file', key='is_empty')
        logging.info(f"Branch decision: is_empty={is_empty}")
        return 'skip_load' if is_empty else 'transform_sales_data'
    except Exception as e:
        logging.error(f"Error in skip_if_empty: {e}")
        raise


branch_task = BranchPythonOperator(
    task_id='branch_on_validation',
    python_callable=skip_if_empty,
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

