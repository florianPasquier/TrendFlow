from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG("dbt_transformation", schedule_interval="@daily", start_date=datetime(2024, 4, 1))

run_dbt = BashOperator(
    task_id="run_dbt",
    bash_command="dbt run --profiles-dir /usr/local/airflow/dbt",
    dag=dag
)
