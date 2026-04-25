from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def load_csv_to_postgres():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    df_orders = pd.read_csv('/opt/airflow/data/orders_raw.csv')
    df_products = pd.read_csv('/opt/airflow/data/products_raw.csv')
    
    # Nahrání produktů (vždy replace)
    df_products.to_sql('raw_products', engine, schema='bronze', if_exists='replace', index=False)
    
    # Inkrementální nahrání objednávek
    try:
        existing_ids = pd.read_sql("SELECT order_id FROM bronze.raw_orders", engine)['order_id'].tolist()
        df_new = df_orders[~df_orders['order_id'].isin(existing_ids)]
        if not df_new.empty:
            df_new.to_sql('raw_orders', engine, schema='bronze', if_exists='append', index=False)
    except:
        df_orders.to_sql('raw_orders', engine, schema='bronze', if_exists='replace', index=False)

default_args = {'owner': 'zay_kein', 'start_date': datetime(2023, 1, 1)}

with DAG('02_load_to_bronze', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    setup = PostgresOperator(task_id='setup', postgres_conn_id='postgres_default', sql="CREATE SCHEMA IF NOT EXISTS bronze;")
    load = PythonOperator(task_id='load', python_callable=load_csv_to_postgres)
    setup >> load
