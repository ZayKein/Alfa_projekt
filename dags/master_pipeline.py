from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG('06_MASTER_ALZA_PIPELINE', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:

    t0 = TriggerDagRunOperator(
        task_id='run_hr_gen', trigger_dag_id='00_hr_generator', wait_for_completion=True)
    t1 = TriggerDagRunOperator(
        task_id='run_order_gen', trigger_dag_id='01_generator_dat', wait_for_completion=True)
    t2 = TriggerDagRunOperator(
        task_id='run_bronze', trigger_dag_id='02_load_to_bronze', wait_for_completion=True)
    t3 = TriggerDagRunOperator(
        task_id='run_silver', trigger_dag_id='03_silver_transformations', wait_for_completion=True)
    t4 = TriggerDagRunOperator(task_id='run_snowflake_raw',
                               trigger_dag_id='04_load_to_snowflake', wait_for_completion=True)
    t5 = TriggerDagRunOperator(task_id='run_snowflake_gold',
                               trigger_dag_id='05_snowflake_gold', wait_for_completion=True)

    t0 >> t1 >> t2 >> t3 >> t4 >> t5
