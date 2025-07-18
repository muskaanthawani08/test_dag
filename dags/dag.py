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
 

# ❄️ Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    'user': '<YOUR_USERNAME>',
    'password': '<YOUR_PASSWORD>',
    'account': '<YOUR_ACCOUNT>.snowflakecomputing.com',
    'warehouse': '<YOUR_WAREHOUSE>',
    'database': '<YOUR_DATABASE>',
    'schema': '<YOUR_SCHEMA>',
    'role': '<YOUR_ROLE>'  # optional
}

# 🚀 Transform and clean the data
def transform_data():
    try:
        # Load raw data
        df = pd.read_pickle('/tmp/daily_sales.pkl')
        logging.info("Original columns: %s", df.columns.tolist())
        logging.info("Data preview before transformation:\n%s", df.head().to_string(index=False))

        # Normalize column names to uppercase
        df.columns = df.columns.str.upper()
        logging.info("Normalized columns: %s", df.columns.tolist())

        # Required columns
        required_columns = ['DATE', 'TIME', 'INVOICE_ID', 'COGS']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logging.error(f"Missing columns: {missing}")
            raise ValueError(f"Missing columns: {missing}")

        # Parse DATE and TIME
        df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
        df['TIME'] = pd.to_datetime(df['TIME'], errors='coerce').dt.time

        # Drop rows with missing required values
        df.dropna(subset=required_columns, inplace=True)

        # Rename column for consistency
        df.rename(columns={'INVOICE_ID': 'Invoice_id'}, inplace=True)

        # Create 'BRACKET' column
        def calculate_bracket(cogs):
            lower = math.floor(cogs / 50) * 50
            upper = lower + 50
            return f"{lower}-{upper}"

        df['BRACKET'] = df['COGS'].apply(calculate_bracket)

        logging.info("Data transformed successfully.")
        logging.info("Transformed data preview:\n%s", df.head().to_string(index=False))

        # Push to Snowflake
        load_data(df, table_name='DAILY_SALES_CLEANED')

    except Exception as e:
        logging.error(f"Error in transform_data: {e}")
        raise

# 📤 Load cleaned data into Snowflake
def load_data(df, table_name):
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        # Create table if not exists
        create_stmt = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Invoice_id STRING,
            STORE STRING,
            CITY STRING,
            CUSTOMER_TYPE STRING,
            GENDER STRING,
            PRODUCT_LINE STRING,
            UNIT_PRICE FLOAT,
            QUANTITY INT,
            TAX_5_PERCENT FLOAT,
            TOTAL FLOAT,
            DATE DATE,
            TIME STRING,
            PAYMENT STRING,
            COGS FLOAT,
            GROSS_MARGIN_PERCENTAGE FLOAT,
            GROSS_INCOME FLOAT,
            RATING FLOAT,
            BRACKET STRING
        );
        """
        cursor.execute(create_stmt)

        # Insert data row by row
        for _, row in df.iterrows():
            insert_stmt = f"""
            INSERT INTO {table_name} VALUES (
                {', '.join(['%s'] * len(row))}
            )
            """
            cursor.execute(insert_stmt, tuple(row))

        conn.commit()
        logging.info(f"Successfully pushed {len(df)} rows to Snowflake table '{table_name}'.")

    except Exception as e:
        logging.error(f"Error loading data to Snowflake: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

# 📥 Download data from Snowflake into a DataFrame
def download_data(table_name):
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        # Query data
        query = f"SELECT * FROM {table_name};"
        cursor.execute(query)

        # Fetch column names and rows
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns)
        logging.info(f"Downloaded {len(df)} rows from Snowflake table '{table_name}'.")
        logging.info("Downloaded data preview:\n%s", df.head().to_string(index=False))

        return df

    except Exception as e:
        logging.error(f"Error downloading data from Snowflake: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


def validate_file():
    try:
        if not os.path.exists('/tmp/daily_sales.pkl'):
            logging.warning("Validation error: Pickle file not found. Treating as empty.")
            open('/tmp/validate_flag.txt', 'w').write('empty')
            return

        df = pd.read_pickle('/tmp/daily_sales.pkl')
        is_empty = df.empty
        flag = 'empty' if is_empty else 'valid'
        open('/tmp/validate_flag.txt', 'w').write(flag)
        logging.info(f"File validation complete: Empty={is_empty}")

    except Exception as e:
        logging.error(f"Error in validate_file: {e}")
        open('/tmp/validate_flag.txt', 'w').write('empty')
        raise

def skip_if_empty():
    try:
        flag = open('/tmp/validate_flag.txt').read()
        logging.info(f"Branch decision: {flag}")
        return 'skip_load' if flag == 'empty' else 'transform_sales_data'
    except Exception as e:
        logging.error(f"Error in skip_if_empty: {e}")
        return 'skip_load'

 
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