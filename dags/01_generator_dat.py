from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import random

def random_date(start_year):
    start = datetime(start_year, 1, 1)
    end = datetime(start_year, 12, 31)
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)

def generuj_alza_data():
    path = "/opt/airflow/data"
    os.makedirs(path, exist_ok=True)
    orders_file = f"{path}/orders_raw.csv"
    products_file = f"{path}/products_raw.csv"

    # 1. Produkty (stále stejné)
    if not os.path.exists(products_file):
        products = pd.DataFrame({
            'product_id': range(1, 51),
            'name': [f'Alza_Product_{i}' for i in range(1, 51)],
            'category': [np.random.choice(['Mobily', 'Laptops', 'Gaming', 'Home']) for _ in range(50)],
            'base_price': np.random.uniform(500, 30000, 50).round(2)
        })
        products.to_csv(products_file, index=False)

    # 2. Objednávky - Historie vs Incremental
    if not os.path.exists(orders_file):
        print("Vytvářím 3letou historii (3000 objednávek)...")
        history = []
        
        # Rok 1 (500 ks)
        for _ in range(500):
            history.append([0, np.random.randint(1, 51), np.random.randint(1, 3), random_date(2022)])
        # Rok 2 (1000 ks)
        for _ in range(1000):
            history.append([0, np.random.randint(1, 51), np.random.randint(1, 3), random_date(2023)])
        # Rok 3 (1500 ks)
        for _ in range(1500):
            history.append([0, np.random.randint(1, 51), np.random.randint(1, 3), random_date(2024)])
        
        df_orders = pd.DataFrame(history, columns=['order_id', 'product_id', 'quantity', 'order_date'])
        df_orders = df_orders.sort_values('order_date')
        df_orders['order_id'] = range(1000, 1000 + len(df_orders))
        
        df_orders.to_csv(orders_file, index=False)
    else:
        print("Přidávám denní dávku (50 objednávek)...")
        existing_orders = pd.read_csv(orders_file)
        start_id = existing_orders['order_id'].max() + 1
        
        new_data = pd.DataFrame({
            'order_id': range(start_id, start_id + 50),
            'product_id': np.random.randint(1, 51, 50),
            'quantity': np.random.randint(1, 3, 50),
            'order_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S') for _ in range(50)]
        })
        new_data.to_csv(orders_file, mode='a', header=False, index=False)

default_args = {'owner': 'zay_kein', 'start_date': datetime(2023, 1, 1)}

with DAG('01_simulace_zdrojovych_dat', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    PythonOperator(task_id='generuj_data', python_callable=generuj_alza_data)
