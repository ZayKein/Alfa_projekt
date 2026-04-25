from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import random

def random_date_seasonal(year):
    month_weights = [1.0, 0.9, 1.1, 1.0, 1.2, 1.1, 1.0, 1.1, 1.4, 1.8, 3.8, 4.8]
    month = random.choices(range(1, 13), weights=month_weights)[0]
    day = random.randint(1, 28)
    hour = random.randint(8, 21)
    return datetime(year, month, day, hour, random.randint(0, 59))

def generuj_alza_data():
    path = "/opt/airflow/data"
    os.makedirs(path, exist_ok=True)
    orders_file = f"{path}/orders_raw.csv"
    products_file = f"{path}/products_raw.csv"

    if not os.path.exists(products_file):
        cat_configs = [
            {'cat': 'Mobily', 'sub': ['iPhone', 'Samsung'], 'min': 8000, 'max': 35000},
            {'cat': 'Laptops', 'sub': ['MacBook', 'Lenovo Legion'], 'min': 15000, 'max': 65000},
            {'cat': 'Gaming', 'sub': ['PS5', 'Xbox', 'Grafická karta'], 'min': 10000, 'max': 25000},
            {'cat': 'Beauty', 'sub': ['Parfém', 'Holicí strojek'], 'min': 500, 'max': 4000}
        ]
        prod_list = []
        for i in range(1, 51):
            cfg = random.choice(cat_configs)
            sub = random.choice(cfg['sub'])
            price = round(float(np.random.uniform(cfg['min'], cfg['max'])), 0)
            prod_list.append([i, f"{sub} Model {i}", cfg['cat'], sub, price])
        pd.DataFrame(prod_list, columns=['product_id', 'name', 'category', 'subcategory', 'base_price']).to_csv(products_file, index=False)

    df_prods = pd.read_csv(products_file)

    if not os.path.exists(orders_file):
        data = []
        counts = [(2022, 500), (2023, 1000), (2024, 1500)]
        for year, count in counts:
            for _ in range(count):
                p_id = np.random.randint(1, 51)
                p_row = df_prods.loc[df_prods['product_id'] == p_id].iloc[0]
                p_price, p_cat = p_row['base_price'], p_row['category']
                
                st, sp, emp_id = 'None', 0, 0
                if p_cat in ['Mobily', 'Laptops', 'Gaming'] and random.random() < 0.25:
                    st = random.choice(['Záruka +1', 'Záruka +3', 'Pojištění'])
                    mult = 0.05 if st == 'Záruka +1' else 0.15 if st == 'Záruka +3' else 0.10
                    sp = round(p_price * mult)
                    emp_id = np.random.randint(1, 21) if random.random() < 0.75 else 0
                
                data.append([0, p_id, np.random.randint(1, 3), st, sp, emp_id, random_date_seasonal(year)])
        
        df_orders = pd.DataFrame(data, columns=['order_id', 'product_id', 'quantity', 'service_type', 'service_price', 'employee_id', 'order_date'])
        df_orders = df_orders.sort_values('order_date')
        df_orders['order_id'] = range(1000, 1000 + len(df_orders))
        df_orders.to_csv(orders_file, index=False)

default_args = {'owner': 'zay_kein', 'start_date': datetime(2023, 1, 1)}
with DAG('01_simulace_zdrojovych_dat', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    PythonOperator(task_id='generuj_data', python_callable=generuj_alza_data)
