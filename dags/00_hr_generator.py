from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import os


def calculate_monthly_salary(age_at_hire, hire_date, current_date):
    y_start = (current_date.year - 2021)
    m_min = 28000 * (1.04 ** y_start)
    m_max = 33000 * (1.04 ** y_start)
    y_exp = (current_date - hire_date).days // 365

    if y_exp >= 3:
        salary = m_max * (1.05 ** 3)
    else:
        if age_at_hire < 20:
            s_base = m_min
        elif age_at_hire <= 25:
            s_base = m_min + (age_at_hire - 20) * ((m_max - m_min) / 5)
        else:
            s_base = m_max
        adj_base = s_base * (1.04 ** y_exp)
        if years_exp := (current_date - hire_date).days // 365 >= 1:
            adj_base = max(adj_base, m_max)
        salary = adj_base * (1.05 ** (y_exp if y_exp < 3 else 3))
    return round(salary)


def generate_hr_data():
    path = "/opt/airflow/data"
    os.makedirs(path, exist_ok=True)
    master_file = f"{path}/employees_master.csv"
    payroll_file = f"{path}/employees_payroll.csv"

    # --- 1. MASTER DATA (ZAMĚSTNANCI) ---
    if os.path.exists(master_file):
        df_master = pd.read_csv(master_file, parse_dates=[
                                'hire_date', 'exit_date'])
        if len(df_master) >= 750:
            print("Zaměstnanci již existují, přeskakuji generování master dat.")
        else:
            # Pokud by jich bylo méně, mohl bys doplňovat, ale pro náš účel stačí kontrola
            pass
    else:
        # Generování úplně poprvé (identické s tvým původním kódem)
        random.seed(42)  # Seed pro konzistenci při případném smazání
        emp_m = []
        f_m = ["Jan", "Petr", "Martin", "Jakub", "Tomáš",
               "Lukáš", "Filip", "David", "Ondřej", "Marek"]
        f_f = ["Jana", "Hana", "Eva", "Lenka", "Kateřina",
               "Alena", "Petra", "Lucie", "Monika", "Adéla"]
        l_m = ["Novák", "Svoboda", "Novotný", "Dvořák", "Černý",
               "Procházka", "Kučera", "Veselý", "Horák", "Němec"]
        l_f = ["Nováková", "Svobodová", "Novotná", "Dvořáková", "Černá",
               "Procházková", "Kučerová", "Veselá", "Horáková", "Němcová"]

        for i in range(1, 751):
            gender = random.choice(['M', 'F'])
            name = f"{random.choice(f_m)} {random.choice(l_m)}" if gender == 'M' else f"{random.choice(f_f)} {random.choice(l_f)}"
            age_now = int(random.triangular(18, 63, 25))
            r = random.random()
            if r < 0.15:
                start_dt = datetime(2021, 1, 1) + \
                    timedelta(days=random.randint(0, 700))
            elif r < 0.45:
                start_dt = datetime(2023, 1, 1) + \
                    timedelta(days=random.randint(0, 700))
            else:
                start_dt = datetime(2025, 1, 1) + \
                    timedelta(days=random.randint(0, 500))

            age_h = max(
                18, age_now - ((datetime.now() - start_dt).days // 365))
            exit_dt = start_dt + \
                timedelta(days=random.randint(180, 1100)
                          ) if random.random() < 0.50 else None
            if exit_dt and exit_dt > datetime.now():
                exit_dt = None

            emp_m.append([i, name, gender, age_now, start_dt, exit_dt])

        df_master = pd.DataFrame(emp_m, columns=[
                                 'employee_id', 'name', 'gender', 'current_age', 'hire_date', 'exit_date'])
        df_master.to_csv(master_file, index=False)

    # --- 2. PAYROLL DATA (MZDY) ---
    existing_payroll = pd.DataFrame()
    if os.path.exists(payroll_file):
        existing_payroll = pd.read_csv(
            payroll_file, parse_dates=['month_year'])
        last_date = existing_payroll['month_year'].max()
        start_sync = (last_date + timedelta(days=32)).replace(day=1)
    else:
        start_sync = df_master['hire_date'].min().replace(day=1)

    new_payroll = []
    today = datetime.now().replace(day=1)

    if start_sync > today:
        print("Všechny mzdy jsou aktuální.")
    else:
        # Procházíme měsíce, které chybí
        curr_m = start_sync
        while curr_m <= today:
            # Filtrujeme lidi, kteří v daném měsíci pracovali
            active_this_month = df_master[
                (df_master['hire_date'] <= curr_m) &
                ((df_master['exit_date'].isna()) |
                 (df_master['exit_date'] >= curr_m))
            ]

            for _, emp in active_this_month.iterrows():
                # Výpočet věku při nástupu pro mzdovou funkci
                age_h = max(
                    18, emp['current_age'] - ((datetime.now() - emp['hire_date']).days // 365))
                salary = calculate_monthly_salary(
                    age_h, emp['hire_date'], curr_m)
                new_payroll.append(
                    [emp['employee_id'], salary, curr_m.strftime('%Y-%m-01')])

            curr_m = (curr_m.replace(day=28) +
                      timedelta(days=4)).replace(day=1)

        if new_payroll:
            df_new = pd.DataFrame(new_payroll, columns=[
                                  'employee_id', 'monthly_salary', 'month_year'])
            df_final_payroll = pd.concat(
                [existing_payroll, df_new], ignore_index=True)
            df_final_payroll.to_csv(payroll_file, index=False)
            print(f"Přidáno {len(new_payroll)} nových mzdových záznamů.")


with DAG('00_hr_generator', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False, tags=['alfa_hr']) as dag:
    PythonOperator(task_id='gen_hr', python_callable=generate_hr_data)
