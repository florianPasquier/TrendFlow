from datetime import datetime
from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor

default_args = {
    'owner': 'data_team',
    'retries': 3
}

with DAG(
    dag_id='dbt_cloud_production_pipeline',
    start_date=datetime(2025, 4, 3),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    trigger_dbt_job = DbtCloudRunJobOperator(
        task_id='trigger_dbt_full_refresh',
        dbt_cloud_conn_id='dbt',
        job_id=70471823449975,  # ID du job dans dbt Cloud
        account_id=70471823451426,  # ID du compte dbt Cloud
        check_interval=300,  # Vérification toutes les 5 min
        timeout=3600,  # Timeout après 1h
        wait_for_termination=True,
        deferrable=True,  # Mode asynchrone
        additional_run_config={
            'generate_docs': True,
            'threads_override': 8,
            'schema_override': 'production'
        }
    )

    monitor_dbt_job = DbtCloudJobRunSensor(
        task_id='monitor_dbt_execution',
        dbt_cloud_conn_id='dbt',
        run_id=trigger_dbt_job.output,
        timeout=3600 * 3,
        mode='reschedule'
    )

    trigger_dbt_job >> monitor_dbt_job