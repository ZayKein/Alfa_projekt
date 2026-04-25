from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    '03_silver_transformations',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['alza_projekt', 'incremental_silver']
) as dag:

    transform_to_silver = PostgresOperator(
        task_id='clean_and_enrich_data_incremental',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE SCHEMA IF NOT EXISTS silver;

            CREATE TABLE IF NOT EXISTS silver.orders_cleaned (
                order_id INT PRIMARY KEY,
                product_id INT,
                employee_id INT,
                service_type TEXT,
                service_price FLOAT,
                category TEXT,
                subcategory TEXT,
                unit_price FLOAT,
                quantity INT,
                total_revenue FLOAT,
                order_date TIMESTAMP
            );

            INSERT INTO silver.orders_cleaned
            SELECT 
                o.order_id,
                o.product_id,
                o.employee_id,
                o.service_type,
                o.service_price,
                p.category,
                p.subcategory,
                p.base_price as unit_price,
                o.quantity,
                ((o.quantity * p.base_price) + o.service_price) as total_revenue,
                CAST(o.order_date AS TIMESTAMP) as order_date
            FROM bronze.raw_orders o
            LEFT JOIN bronze.raw_products p ON o.product_id = p.product_id
            WHERE o.order_id > (SELECT COALESCE(MAX(order_id), 0) FROM silver.orders_cleaned);
        """
    )

    transform_to_silver
