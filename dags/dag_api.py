from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator
)



def ingest_api(**context):
    """
    Ingest data from the API and return the results as a JSON string.
    """
    import serpapi
    import json
    import pandas as pd
    import os

    client = serpapi.Client(api_key=os.getenv("SERPAPI_KEY"))

    params = {
        "engine": os.getenv("ENGINE"),
        "geo": "FR",
        "date": "now 24-H",
        "cat": 0,
        "csv": 1,
    }

    def get_trends(params):
        search = client.search(params)
        results = search.as_dict()
        return results

    results = get_trends(params)
    with open('/tmp/google_trends.json', 'w',encoding='utf-8') as f:
        json.dump(results,f, indent=2, ensure_ascii=False)
    return '/tmp/google_trends.json'


with DAG(
    dag_id="dag_api",
    start_date=datetime(2025, 3, 24),
    default_args={
        "retries": 2,
        "email_on_failure": False,
        "start_date": datetime(2025, 3, 24),
    },
    catchup=False,
    schedule_interval="@daily",
) as dag:

    ingest_api_task = PythonOperator(
        task_id="ingest_api",
        python_callable=ingest_api,
        dag=dag,
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/tmp/google_trends.json",
        dst="google_trends.json",
        bucket="trendflow-455409-trendflow-bucket",
        gcp_conn_id="gcp_airflow"
    )

    ingest_api_task >> upload_to_gcs_task
