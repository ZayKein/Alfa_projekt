from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def push_everything_to_snowflake():
    # Připojení k lokálnímu Postgresu
    pg_engine = create_engine(
        'postgresql://airflow:airflow@postgres:5432/airflow')

    # Připojení k Snowflake
    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    sf_engine = sf_hook.get_sqlalchemy_engine()

    # Seznam tabulek k exportu (Zdroj v Postgres -> Cíl ve Snowflake)
    tables = {
        'silver.products_dim': 'DIM_PRODUCTS',
        'silver.employees_dim': 'DIM_EMPLOYEES',
        'silver.payroll_fact': 'FACT_PAYROLL',
        'silver.orders_cleaned': 'FACT_SALES'
    }

    # Pro jistotu vynutíme databázi a schéma v Snowflake
    conn = sf_hook.get_conn()
    conn.cursor().execute("USE DATABASE ALFA_PROJEKT")
    conn.cursor().execute("USE SCHEMA RAW")

    for pg_table, sf_table in tables.items():
        print(f"Nahrávám {pg_table} do Snowflake jako {sf_table}...")
        df = pd.read_sql(f"SELECT * FROM {pg_table}", pg_engine)

        # 1. Sloupce chceme VELKÝMI (pro DAG 05)
        df.columns = [x.upper() for x in df.columns]

        # 2. Název tabulky pošleme MALÝMI (SQLAlchemy/Snowflake standard)
        # Snowflake ji interně uloží jako velkou, pokud nepoužijeme uvozovky
        df.to_sql(
            name=sf_table.lower(),
            con=sf_engine,
            schema='RAW',
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=5000
        )
        print(f"Tabulka {sf_table} hotova.")


with DAG(
    '04_load_to_snowflake',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['cloud', 'final']
) as dag:

    task_push = PythonOperator(
        task_id='push_all_to_snowflake',
        python_callable=push_everything_to_snowflake
    )
