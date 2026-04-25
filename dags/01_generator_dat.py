from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import os


def generuj_alza_data():
    path = "/opt/airflow/data"
    df_e = pd.read_csv(f"{path}/employees_master.csv",
                       parse_dates=['hire_date', 'exit_date'])

    # 1. PRODUKTY S REÁLNÝMI MODELY
    prods = []
    # Definice modelů pro každou subkategorii
    models_dict = {
        'iPhony': ['iPhone 13', 'iPhone 14', 'iPhone 15', 'iPhone 15 Pro Max'],
        'Android telefony': ['Samsung Galaxy S24', 'Xiaomi 14', 'Google Pixel 8', 'Motorola Edge'],
        'Příslušenství': ['AirPods Pro', 'Pouzdro MagSafe', 'Nabíječka 20W', 'Samsung Galaxy Buds'],
        'MacBooky': ['MacBook Air M2', 'MacBook Air M3', 'MacBook Pro 14"', 'MacBook Pro 16"'],
        'Herní notebooky': ['ASUS ROG Zephyrus', 'Lenovo Legion 5', 'MSI Katana', 'Acer Predator'],
        'Kancelářské': ['HP Pavilion', 'Dell Latitude', 'Lenovo ThinkPad', 'ASUS Vivobook'],
        'Konzole': ['PlayStation 5', 'Xbox Series X', 'Nintendo Switch OLED', 'Steam Deck'],
        'Hry': ['Elden Ring', 'FIFA 24', 'Spider-Man 2', 'God of War Ragnarok'],
        'Parfémy': ['Dior Sauvage', 'Chanel No. 5', 'Hugo Boss Bottled', 'Armani Acqua di Gio'],
        'Elektro pro krásu': ['Dyson Airwrap', 'Philips Lumea', 'Braun Series 9', 'Oral-B iO']
    }

    # Mapování subkategorií na hlavní kategorie
    cat_mapping = {
        'Mobily': ['iPhony', 'Android telefony', 'Příslušenství'],
        'Laptops': ['MacBooky', 'Herní notebooky', 'Kancelářské'],
        'Gaming': ['Konzole', 'Hry'],
        'Beauty': ['Parfémy', 'Elektro pro krásu']
    }

    # Vygenerujeme 100 unikátních produktů
    for i in range(1, 101):
        m_cat = random.choice(list(cat_mapping.keys()))
        s_cat = random.choice(cat_mapping[m_cat])
        model_name = random.choice(models_dict[s_cat])

        # Logika cen
        if s_cat in ['iPhony', 'MacBooky', 'Herní notebooky']:
            price = random.randint(25000, 65000)
        elif s_cat in ['Android telefony', 'Konzole', 'Kancelářské', 'Elektro pro krásu']:
            price = random.randint(8000, 24000)
        else:
            price = random.randint(500, 5000)

        prods.append([i, f"{model_name} (id:{i})", m_cat,
                     s_cat, price, round(price * 0.75)])

    df_p = pd.DataFrame(prods, columns=[
                        'product_id', 'name', 'category', 'subcategory', 'base_price', 'unit_cost'])
    df_p.to_csv(f"{path}/products_raw.csv", index=False)

    # 2. GENERÁTOR OBJEDNÁVEK
    growth = {2022: (30, 60), 2023: (50, 100), 2024: (
        90, 200), 2025: (160, 300), 2026: (190, 450)}
    order_data, oid = [], 100001
    for year, (min_d, max_d) in growth.items():
        curr = datetime(year, 1, 1)
        end_y = datetime(year, 12, 31) if year < 2026 else datetime.now()
        while curr <= end_y:
            active = df_e[(df_e['hire_date'] <= curr) & (
                (df_e['exit_date'].isna()) | (df_e['exit_date'] > curr))].to_dict('records')
            count = int(random.randint(min_d, max_d) *
                        (1.6 if curr.month == 12 else 1.0))
            for _ in range(count):
                p = df_p.sample(1).iloc[0]  # Opravený přístup k řádku
                emp_id, st, sp = 0, 'None', 0
                if active and random.random() < 0.45:
                    e = random.choice(active)
                    emp_id, yrs = e['employee_id'], (
                        curr - pd.to_datetime(e['hire_date'])).days // 365
                    chance = 0.35 if yrs >= 3 else (0.20 if yrs >= 1 else 0.10)
                    if p['category'] in ['Mobily', 'Laptops', 'Gaming'] and random.random() < chance:
                        st, sp = random.choice(
                            ['Záruka+1', 'Pojištění']), round(p['base_price'] * 0.12)
                order_data.append([oid, p['product_id'], random.randint(
                    1, 2), st, sp, emp_id, curr.strftime('%Y-%m-%d %H:%M:%S')])
                oid += 1
            curr += timedelta(days=1)

    pd.DataFrame(order_data, columns=['order_id', 'product_id', 'quantity', 'service_type',
                 'service_price', 'employee_id', 'order_date']).to_csv(f"{path}/orders_raw.csv", index=False)


default_args = {'owner': 'zay_kein', 'start_date': datetime(2023, 1, 1)}
with DAG('01_generator_dat', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    PythonOperator(task_id='gen_orders', python_callable=generuj_alza_data)
