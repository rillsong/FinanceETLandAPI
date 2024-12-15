import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
# from src import etl as ETL
import src.etl as ETL
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta
import yaml

# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

sp500_tickers = pd.read_csv(os.path.join(os.path.dirname(__file__),"tickers.csv"),usecols=["Symbol"]).squeeze().tolist()
yfExtractObj = ETL.yfinanceExtractor(ETL.YF_SESSION)

conn = BaseHook.get_connection('financial_data_conn')  # Use the connection ID you defined
DB_URL = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
# r"mysql+pymysql://airflow:airflow@mysql/airflow"
# Retrieve connection info for financial data
dl = ETL.DataLoader(url = DB_URL,conn_pool_size = 20, max_overflow = 30)

# to run everyday, replace with today()
start_date = '2023-12-10'
end_date = '2024-12-10'


with DAG(
    dag_id = 'daily_price_volume_etl',
    default_args=default_args,
    description='ETL pipeline for fetching and processing daily price and volume data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as daily_price_volume_etl:
    
    # Step 1: Fetch raw data and save to landing folder
    fetch_raw_data = PythonOperator(
        task_id='fetch_raw_data',
        python_callable=ETL.save_raw_data,
        provide_context=True,
        op_kwargs={
            'yfExtractObj': yfExtractObj,
            'tickers': sp500_tickers,
            'start_date': start_date,
            'end_date': end_date,
            'landing_folder': None,
        },
    )
    
    get_missing_tikers_task = PythonOperator(
        task_id='get_missing_tikers',
        python_callable=ETL.get_missing_tikers,
        op_kwargs={'all_tickers': sp500_tickers},
    )

    # Step 2: Apply transformations and save processed data
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=ETL.transform_and_save_data,
        provide_context=True,
        op_kwargs={
            'raw_data_path': "{{ ti.xcom_pull(task_ids='fetch_raw_data', key='raw_data_path') }}",
            'processed_folder': None,
        },
    )

    # Step 3: Upsert data to the database
    upsert_data = PythonOperator(
        task_id='upsert_data_to_db',
        python_callable=ETL.upsert_to_db,
        provide_context=True,
        op_kwargs={
            'dataLoaderObj': dl,
            'processed_data_path': "{{ ti.xcom_pull(task_ids='transform_data', key='processed_data_path') }}",
        },
    )
    
    # Set task dependencies
    fetch_raw_data >> get_missing_tikers_task >> transform_data >> upsert_data
    

with DAG(
    dag_id = 'create_daily_price_volume_table',
    default_args=default_args,
    description='creating table daily price and volume data',
    catchup=False,
    schedule_interval=None, # manual trigger
    start_date = datetime(2024,12,1)
) as create_table_dag:
    create_db_task = PythonOperator(
        task_id='create_price_volume_table',
        python_callable=dl.create_price_volume_table,
        provide_context=True,
    )
    create_db_task


# with DAG(
#     dag_id = 'daily_price_volume_etl_2',
#     default_args=default_args,
#     description='ETL pipeline for fetching and processing daily price and volume data',
#     schedule_interval='@daily',
#     start_date=datetime(2024, 12, 1),
#     catchup=False,
# ) as daily_price_volume_etl_2:
#     run_pipeline = PythonOperator(
#         task_id = 'run_initial_pipeline',
#         python_callable = ETL.run_daily_pipeline,
#         provide_context=True,
#         op_kwargs={'yfExtractObj':yfExtractObj,'dataLoaderObj':dl,
#             'tickers': sp500_tickers, 'start_date': start_date, 'end_date': end_date}
#         )
    
#     run_pipeline 

