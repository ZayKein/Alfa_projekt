from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

def generuj_alza_data():
    # Cesta uvnitř kontejneru Airflow
    path = "/opt/airflow/data"
    if not os.path.exists(path):
        os.makedirs(path)
    
    # 1. Generujeme Produkty
    products = pd.DataFrame({
        'product_id': range(1, 51),
        'name': [f'Alza_Product_{i}' for i in range(1, 51)],
        'category': [np.random.choice(['Mobily', 'Laptops', 'Gaming', 'Home']) for _ in range(50)],
        'base_price': np.random.uniform(500, 30000, 50).round(2)
    })
    products.to_csv(f"{path}/products_raw.csv", index=False)

    # 2. Generujeme Objednávky (posledních 7 dní)
    orders = pd.DataFrame({
        'order_id': range(1000, 1500),
        'product_id': np.random.randint(1, 51, 500),
        'quantity': np.random.randint(1, 3, 500),
        'order_date': [datetime.now() - timedelta(days=np.random.randint(0, 7)) for _ in range(500)]
    })
    orders.to_csv(f"{path}/orders_raw.csv", index=False)
    print("Soubory products_raw.csv a orders_raw.csv byly vytvořeny.")

default_args = {
    'owner': 'zay_kein',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    '01_simulace_zdrojovych_dat',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['alza_projekt', 'step_1']
) as dag:

    task_generate = PythonOperator(
        task_id='generuj_csv_ze_zdroje',
        python_callable=generuj_alza_data
    )
