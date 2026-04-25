from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def load_csv_to_postgres():
    # Připojení k DB uvnitř Dockeru
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    # Cesty k souborům, které vygeneroval DAG 01
    path = '/opt/airflow/data'
    
    # 1. Nahrání Produktů
    df_products = pd.read_csv(f'{path}/products_raw.csv')
    df_products.to_sql('raw_products', engine, schema='bronze', if_exists='replace', index=False)
    
    # 2. Nahrání Objednávek (těch tvých 3000+ řádků)
    df_orders = pd.read_csv(f'{path}/orders_raw.csv')
    df_orders.to_sql('raw_orders', engine, schema='bronze', if_exists='replace', index=False)
    
    print("Data úspěšně přenesena z CSV do Postgres schématu BRONZE.")

default_args = {
    'owner': 'zay_kein',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    '02_load_to_bronze',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['alza_projekt', 'ingestion']
) as dag:

    # Úkol A: Příprava schémat v DB
    setup_db = PostgresOperator(
        task_id='setup_db_structure',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE SCHEMA IF NOT EXISTS bronze;
            CREATE SCHEMA IF NOT EXISTS silver;
            CREATE SCHEMA IF NOT EXISTS gold;
        """
    )

    # Úkol B: Samotné nahrání dat přes Python/Pandas
    ingest_data = PythonOperator(
        task_id='ingest_csv_to_bronze',
        python_callable=load_csv_to_postgres
    )

    setup_db >> ingest_data
