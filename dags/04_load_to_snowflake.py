airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def postgres_to_snowflake():
    # 1. Načtení dat z tvého lokálního Postgresu (Silver vrstva)
    # Host je 'postgres', protože Airflow i DB jsou v jedné Docker síti
    pg_engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    df = pd.read_sql("SELECT * FROM silver.orders_cleaned", pg_engine)
    
    print(f"Načteno {len(df)} řádků z Postgresu. Zahajuji upload do Snowflake...")

    # 2. Odeslání do Cloudu (Snowflake)
    # Používáme tvou otestovanou 'snowflake_conn'
    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    sf_hook.write_pandas(
        df=df,
        table_name='ORDERS_STAGING',
        database='ALZA_PROJEKT',
        schema='RAW',
        overwrite=True # Při každém spuštění tabulku v cloudu přepíšeme čerstvými daty
    )
    print("Upload dokončen! Data jsou ve Snowflake.")

with DAG(
    '04_load_to_snowflake',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['alza_projekt', 'cloud_export']
) as dag:

    task_push = PythonOperator(
        task_id='push_data_to_cloud',
        python_callable=postgres_to_snowflake
