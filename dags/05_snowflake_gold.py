from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Definice SQL příkazů s novou normalizovanou strukturou
SQL_GOLD_TRANSFORM = """
USE DATABASE ALZA_PROJEKT;
USE SCHEMA GOLD;

-- 1. Inkrementální nahrání Faktů o prodejích (odlehčená verze bez Revenue/Cost)
INSERT INTO ALZA_PROJEKT.GOLD.FACT_SALES (ORDER_ID, PRODUCT_ID, EMPLOYEE_ID, DATE_KEY, QUANTITY, SERVICE_TYPE, SERVICE_PRICE)
SELECT 
    ORDER_ID, 
    PRODUCT_ID, 
    EMPLOYEE_ID, 
    CAST(ORDER_DATE AS DATE),
    QUANTITY, 
    SERVICE_TYPE, 
    SERVICE_PRICE
FROM ALZA_PROJEKT.RAW.FACT_SALES
WHERE ORDER_ID > (SELECT COALESCE(MAX(ORDER_ID), 0) FROM ALZA_PROJEKT.GOLD.FACT_SALES);

-- 2. Inkrementální nahrání mezd (Payroll Fact)
INSERT INTO ALZA_PROJEKT.GOLD.FACT_PAYROLL (EMPLOYEE_ID, MONTHLY_SALARY, DATE_KEY)
SELECT 
    EMPLOYEE_ID, 
    MONTHLY_SALARY, 
    CAST(MONTH_YEAR AS DATE)
FROM ALZA_PROJEKT.RAW.FACT_PAYROLL
WHERE (EMPLOYEE_ID, CAST(MONTH_YEAR AS DATE)) NOT IN (SELECT EMPLOYEE_ID, DATE_KEY FROM ALZA_PROJEKT.GOLD.FACT_PAYROLL);

-- 3. Merge Dimenze Zaměstnanců
MERGE INTO ALZA_PROJEKT.GOLD.DIM_EMPLOYEES target
USING ALZA_PROJEKT.RAW.DIM_EMPLOYEES source
ON target.EMPLOYEE_ID = source.EMPLOYEE_ID
WHEN MATCHED THEN UPDATE SET 
    target.EXIT_DATE = source.EXIT_DATE,
    target.IS_ACTIVE = CASE WHEN source.EXIT_DATE IS NULL THEN TRUE ELSE FALSE END,
    -- Tady používáme AGE, protože tak se jmenuje v RAW (ze Silveru)
    target.CURRENT_AGE = source.AGE + DATEDIFF(year, CAST(source.HIRE_DATE AS DATE), CURRENT_DATE())
WHEN NOT MATCHED THEN INSERT (EMPLOYEE_ID, EMPLOYEE_NAME, GENDER, AGE_AT_HIRE, CURRENT_AGE, HIRE_DATE, EXIT_DATE, IS_ACTIVE)
VALUES (
    source.EMPLOYEE_ID, 
    source.NAME, 
    source.GENDER, 
    source.AGE, -- AGE_AT_HIRE
    source.AGE + DATEDIFF(year, CAST(source.HIRE_DATE AS DATE), CURRENT_DATE()), -- CURRENT_AGE
    source.HIRE_DATE, 
    source.EXIT_DATE, 
    CASE WHEN source.EXIT_DATE IS NULL THEN TRUE ELSE FALSE END
);

-- 4. Merge Dimenze Produktů (včetně cen a nákladů)
MERGE INTO ALZA_PROJEKT.GOLD.DIM_PRODUCTS target
USING ALZA_PROJEKT.RAW.DIM_PRODUCTS source
ON target.PRODUCT_ID = source.PRODUCT_ID
WHEN MATCHED THEN UPDATE SET 
    target.PRODUCT_NAME = source.NAME, 
    target.CATEGORY = source.CATEGORY, 
    target.SUBCATEGORY = source.SUBCATEGORY,
    target.UNIT_PRICE = source.BASE_PRICE,
    target.UNIT_COST = source.UNIT_COST
WHEN NOT MATCHED THEN INSERT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUBCATEGORY, UNIT_PRICE, UNIT_COST)
VALUES (source.PRODUCT_ID, source.NAME, source.CATEGORY, source.SUBCATEGORY, source.BASE_PRICE, source.UNIT_COST);
"""

with DAG('05_snowflake_gold', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    SnowflakeOperator(
        task_id='run_gold_transform',
        snowflake_conn_id='snowflake_conn',
        sql=SQL_GOLD_TRANSFORM
    )
