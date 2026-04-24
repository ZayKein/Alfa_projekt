from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    '03_silver_transformations',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['alza_projekt', 'step_3']
) as dag:

    # Transformace Objednávek (Silver vrstva)
    # - Přidáváme výpočet tržby (quantity * price)
    # - Čistíme datové typy
    transform_orders = PostgresOperator(
        task_id='transform_orders_to_silver',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS silver.orders_cleaned AS
            SELECT 
                o.order_id,
                o.product_id,
                p.category,
                o.quantity,
                p.base_price as unit_price,
                (o.quantity * p.base_price) as total_revenue,
                CAST(o.order_date AS TIMESTAMP) as order_date
            FROM bronze.raw_orders o
            LEFT JOIN bronze.raw_products p ON o.product_id = p.product_id
            WHERE o.quantity > 0;
            
            -- Pokud tabulka už existuje, pro tento projekt ji jen promažeme a naplníme znovu
            TRUNCATE silver.orders_cleaned;
            INSERT INTO silver.orders_cleaned
            SELECT 
                o.order_id,
                o.product_id,
                p.category,
                o.quantity,
                p.base_price,
                (o.quantity * p.base_price),
                CAST(o.order_date AS TIMESTAMP)
            FROM bronze.raw_orders o
            LEFT JOIN bronze.raw_products p ON o.product_id = p.product_id
            WHERE o.quantity > 0;
        """
    )

    transform_orders
