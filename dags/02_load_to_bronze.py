from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine


def load_all_to_bronze():
    engine = create_engine(
        'postgresql://airflow:airflow@postgres:5432/airflow')
    path, files = '/opt/airflow/data', {'raw_products': 'products_raw.csv', 'raw_orders': 'orders_raw.csv',
                                        'raw_employees': 'employees_master.csv', 'raw_payroll': 'employees_payroll.csv'}
    for table, file in files.items():
        fp = f"{path}/{file}"
        if os.path.exists(fp):
            df = pd.read_csv(fp)
            if table == 'raw_orders':
                try:
                    existing = pd.read_sql(f"SELECT order_id FROM bronze.{table}", engine)[
                        'order_id'].tolist()
                    df[~df['order_id'].isin(existing)].to_sql(
                        table, engine, schema='bronze', if_exists='append', index=False)
                except:
                    df.to_sql(table, engine, schema='bronze',
                              if_exists='replace', index=False)
            else:
                df.to_sql(table, engine, schema='bronze',
                          if_exists='replace', index=False)


with DAG('02_load_to_bronze', start_date=datetime(2023, 1, 1), schedule_interval=None) as dag:
    s = PostgresOperator(task_id='setup', postgres_conn_id='postgres_default',
                         sql="CREATE SCHEMA IF NOT EXISTS bronze; CREATE SCHEMA IF NOT EXISTS silver;")
    l = PythonOperator(task_id='load', python_callable=load_all_to_bronze)
    s >> l
