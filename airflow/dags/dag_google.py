from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
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
    with open("/tmp/google_trends.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # push the path to the xcom
    context["task_instance"].xcom_push(key="path", value="/tmp/google_trends.json")
    return "/tmp/google_trends.json"


def convert_to_csv(**context):
    import pandas as pd
    from pandas import json_normalize
    import json

    path = context["task_instance"].xcom_pull(task_ids="ingest_api", key="path")

    with open(path) as f:
        data = json.load(f)

    df = json_normalize(data["trending_searches"])
    # Flatten the 'categories' column
    df["categories"] = df["categories"].apply(lambda x: [d["name"] for d in x])
    # Drop the 'serpapi_google_trends_link' column
    df.drop(columns=["serpapi_google_trends_link"], inplace=True)
    # Add columns 'source' and 'currentness'
    df["source"] = "google_trends"
    df["currentness"] = pd.to_datetime(df["start_timestamp"], unit="s").dt.strftime(
        "%Y-%m-%d-%H:%M:%S"
    )

    # Rename column 'query' to 'name'
    df = df.rename(
        columns={
            "query": "name",
            "search_volume": "count",
            "trend_breakdown": "associated_entities",
        }
    )
    # Replace the character "\u2014"(em dash) with "-" in the 'name' column
    df['name'] = df['name'].str.replace("\u2013", "-")

    # Drop columns: 'increase_percentage', 'end_timestamp'
    df = df.drop(
        columns=["increase_percentage", "end_timestamp", "start_timestamp", "active"]
    )
    df.to_csv("/tmp/google_trends.csv", index=False, encoding="utf-8", sep=";")
    return "/tmp/google_trends.csv"


with DAG(
    dag_id="dag_google",
    start_date=datetime(2025, 3, 24),
    default_args={
        "retries": 2,
        "email_on_failure": False,
        "start_date": datetime(2025, 3, 24),
        "owner": "Trendflow",
    },
    catchup=False,
    schedule_interval="@daily",
) as dag:

    ingest_api_task = PythonOperator(
        task_id="ingest_api",
        python_callable=ingest_api,
        dag=dag,
    )

    convert_to_csv_task = PythonOperator(
        task_id="convert_to_csv",
        python_callable=convert_to_csv,
        dag=dag,
    )
    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/tmp/google_trends.csv",
        dst="google_trends.csv",
        bucket="trendflow-455409-trendflow-bucket",
        gcp_conn_id="gcp_airflow",
    )

    ingest_api_task >> convert_to_csv_task >> upload_to_gcs_task
