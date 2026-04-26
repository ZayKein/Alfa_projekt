from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    '03_silver_transformations',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['silver', 'alfa_enterprise']
) as dag:

    # 1. Produkty (Dimenze)
    t_products = PostgresOperator(
        task_id='silver_products',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS silver.products_dim (
                product_id INT PRIMARY KEY,
                name TEXT,
                category TEXT,
                subcategory TEXT,
                base_price FLOAT,
                unit_cost FLOAT
            );
            TRUNCATE silver.products_dim;
            INSERT INTO silver.products_dim
            SELECT product_id, name, category, subcategory, base_price, unit_cost
            FROM bronze.raw_products;
        """
    )

    # 2. Zaměstnanci (Dimenze)
    t_employees = PostgresOperator(
        task_id='silver_employees',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS silver.employees_dim (
                employee_id INT PRIMARY KEY,
                name TEXT,
                gender TEXT,
                age INT,
                hire_date DATE,
                exit_date DATE,
                is_active BOOLEAN
            );
            TRUNCATE silver.employees_dim;
            INSERT INTO silver.employees_dim
            SELECT employee_id, name, gender, current_age, CAST(hire_date AS DATE), CAST(exit_date AS DATE),
            CASE WHEN exit_date IS NULL THEN TRUE ELSE FALSE END FROM bronze.raw_employees;
        """
    )

    # 3. Mzdy (Fakta o nákladech)
    t_payroll = PostgresOperator(
        task_id='silver_payroll',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS silver.payroll_fact (
                employee_id INT,
                monthly_salary FLOAT,
                month_year DATE
            );
            TRUNCATE silver.payroll_fact;
            INSERT INTO silver.payroll_fact
            SELECT employee_id, monthly_salary, CAST(month_year AS DATE)
            FROM bronze.raw_payroll;
        """
    )

    # 4. Objednávky (Fakta o prodejích) - Odlehčená verze
    t_orders = PostgresOperator(
        task_id='silver_orders',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS silver.orders_cleaned (
                order_id INT PRIMARY KEY,
                product_id INT,
                employee_id INT,
                quantity INT,
                service_type TEXT,
                service_price FLOAT,
                order_date TIMESTAMP
            );
            INSERT INTO silver.orders_cleaned
            SELECT 
                o.order_id, 
                o.product_id, 
                o.employee_id, 
                o.quantity, 
                o.service_type,
                o.service_price,
                CAST(o.order_date AS TIMESTAMP)
            FROM bronze.raw_orders o
            WHERE o.order_id > (SELECT COALESCE(MAX(order_id), 0) FROM silver.orders_cleaned);
        """
    )

    # Všechno běží nezávisle a paralelně!
    [t_products, t_employees, t_payroll, t_orders]
