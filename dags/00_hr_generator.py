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
        salary = m_max * (1.05 ** 3)  # Elitní senioritní strop
    else:
        if age_at_hire < 20:
            s_base = m_min
        elif age_at_hire <= 25:
            s_base = m_min + (age_at_hire - 20) * ((m_max - m_min) / 5)
        else:
            s_base = m_max
        adj_base = s_base * (1.04 ** y_exp)
        if y_exp >= 1:
            adj_base = max(adj_base, m_max)
        salary = adj_base * (1.05 ** y_exp)
    return round(salary)


def generate_hr_data():
    path = "/opt/airflow/data"
    os.makedirs(path, exist_ok=True)
    emp_m, pay_h = [], []
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

        age_h = max(18, age_now - ((datetime.now() - start_dt).days // 365))
        exit_dt = start_dt + \
            timedelta(days=random.randint(180, 1100)
                      ) if random.random() < 0.50 else None
        if exit_dt and exit_dt > datetime.now():
            exit_dt = None
        emp_m.append([i, name, gender, age_now, start_dt.strftime(
            '%Y-%m-%d'), exit_dt.strftime('%Y-%m-%d') if exit_dt else None])

        curr_m = start_dt.replace(day=1)
        limit_dt = exit_dt if exit_dt else datetime.now()
        while curr_m <= limit_dt:
            salary = calculate_monthly_salary(age_h, start_dt, curr_m)
            pay_h.append([i, salary, curr_m.strftime('%Y-%m-01')])
            curr_m = (curr_m.replace(day=28) +
                      timedelta(days=4)).replace(day=1)

    pd.DataFrame(emp_m, columns=['employee_id', 'name', 'gender', 'current_age',
                 'hire_date', 'exit_date']).to_csv(f"{path}/employees_master.csv", index=False)
    pd.DataFrame(pay_h, columns=['employee_id', 'monthly_salary', 'month_year']).to_csv(
        f"{path}/employees_payroll.csv", index=False)


with DAG('00_hr_generator', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    PythonOperator(task_id='gen_hr', python_callable=generate_hr_data)
