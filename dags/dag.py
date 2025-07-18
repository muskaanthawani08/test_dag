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
 
 
def download_data():
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
 
        df.to_pickle('/tmp/daily_sales.pkl')
        logging.info(f"Fetched {len(df)} rows from Snowflake.")
        logging.info("data preview after pushing:\n%s", df.head().to_string(index=False))  
 
    except Exception as e:
        logging.error(f"Snowflake error: {e}")
        raise
 
    finally:
        cur.close()
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
 
 
def transform_data():
    try:
        # Load data
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

        logging.info("Column types before parsing:\n%s", df.dtypes)

        # Parse DATE
        df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce').dt.date  # 🔧 CHANGED: convert to pure date

        # Parse TIME safely
        def parse_time(t):
            try:
                return datetime.strptime(str(t), '%H:%M:%S').strftime('%H:%M:%S')  # 🔧 CHANGED: convert to string
            except Exception:
                return None  # 🔧 CHANGED: use None instead of pd.NaT for string compatibility

        df['TIME'] = df['TIME'].apply(parse_time)

        # Log distinct TIME values
        distinct_times = df['TIME'].dropna().unique()
        logging.info("Distinct TIME values:\n%s", distinct_times)

        # Drop rows with missing required fields
        logging.info("Missing values in required columns:\n%s", df[required_columns].isnull().sum())
        logging.info("Rows before dropna: %d", len(df))
        df.dropna(subset=required_columns, inplace=True)
        logging.info("Rows after dropna: %d", len(df))

        # Create 'BRACKET' column
        def calculate_bracket(cogs):
            try:
                cogs = float(cogs)
                lower = math.floor(cogs / 50) * 50
                upper = lower + 50
                return f"{lower}-{upper}"
            except Exception:
                return "Invalid"

        df['BRACKET'] = df['COGS'].apply(calculate_bracket)

        # Save cleaned data
        df.to_pickle('/tmp/daily_sales_cleaned.pkl')
        logging.info("Data transformed successfully.")
        logging.info("Transformed data preview:\n%s", df.head().to_string(index=False))

    except Exception as e:
        logging.error(f"Error in transform_data: {e}")
        raise


def load_data():
    df = pd.read_pickle('/tmp/daily_sales_cleaned.pkl')

    try:
        table = 'DAILY_SALES_OP'

        logging.info(f"Connecting to Snowflake ")

        conn = sf.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )

        snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
        snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')

        cur = conn.cursor()
        cur.execute(f"USE DATABASE {snowflake_database}")
        cur.execute(f"USE SCHEMA {snowflake_schema}")

        create_stmt = f"""
        CREATE OR REPLACE TABLE {table} (
            INVOICE_ID STRING,
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

        cur.execute(create_stmt)

        # 🔧 CHANGED: Ensure all values are converted to compatible types
        data = [
            (
                str(row['INVOICE_ID']), str(row['STORE']), str(row['CITY']), str(row['CUSTOMER_TYPE']), str(row['GENDER']),
                str(row['PRODUCT_LINE']), float(row['UNIT_PRICE']), int(row['QUANTITY']), float(row['TAX_5_PERCENT']), float(row['TOTAL']),
                row['DATE'], row['TIME'], str(row['PAYMENT']), float(row['COGS']), float(row['GROSS_MARGIN_PERCENTAGE']),
                float(row['GROSS_INCOME']), float(row['RATING']), str(row['BRACKET'])
            )
            for _, row in df.iterrows()
        ]

        insert_stmt = f"""
        INSERT INTO {table} (
            INVOICE_ID, STORE, CITY, CUSTOMER_TYPE, GENDER, PRODUCT_LINE,
            UNIT_PRICE, QUANTITY, TAX_5_PERCENT, TOTAL, DATE, TIME, PAYMENT,
            COGS, GROSS_MARGIN_PERCENTAGE, GROSS_INCOME, RATING, BRACKET
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cur.executemany(insert_stmt, data)
        conn.commit()
        logging.info(f"Successfully loaded {len(df)} rows into Snowflake table {table}.")

    except Exception as e:
        logging.error(f"Snowflake error: {e}")
        raise

    finally:
        cur.close()
        conn.close()



 
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
 
# def skip_if_empty(**kwargs):
#     try:
#         is_empty = kwargs['ti'].xcom_pull(task_ids='validate_sales_file', key='is_empty')
#         logging.info(f"Branch decision: is_empty={is_empty}")
#         return 'skip_load' if is_empty else 'transform_sales_data'
#     except Exception as e:
#         logging.error(f"Error in skip_if_empty: {e}")
#         raise
 
 
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