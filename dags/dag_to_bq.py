from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


with DAG(
    "to_biq_query",
    schedule_interval="@daily",
    start_date=datetime(2025, 3, 24),
    catchup=False,
) as dag:

    x_trends_to_bq = GCSToBigQueryOperator(
        task_id="x_trends_to_bq",
        bucket="trendflow-455409-trendflow-bucket",
        source_objects=["x_trends.csv"],
        destination_project_dataset_table="trendflow-455409.trendflow.trends",
        schema_fields=[
            {"name": "currentness", "type": "STRING", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "source", "type": "STRING", "mode": "REQUIRED"},
            {"name": "categories", "type": "STRING", "mode": "NULLABLE"},
            {"name": "associated_entities", "type": "STRING", "mode": "NULLABLE"},
        ],
        create_disposition="CREATE_IF_NEEDED",  # This will create the table if it doesn't exist
        write_disposition="WRITE_APPEND",  # This will overwrite the table if it exists
        skip_leading_rows=1,  # Skip header row
        source_format="CSV",
        field_delimiter=";",
        autodetect=False,
        gcp_conn_id="gcp_airflow",
        dag=dag,
    )
    
    g_trends_to_bq = GCSToBigQueryOperator(
        task_id="g_trends_to_bq",
        bucket="trendflow-455409-trendflow-bucket",
        source_objects=["google_trends.csv"],
        destination_project_dataset_table="trendflow-455409.trendflow.trends",
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "categories", "type": "STRING", "mode": "NULLABLE"},
            {"name": "associated_entities", "type": "STRING", "mode": "NULLABLE"},
            {"name": "source", "type": "STRING", "mode": "REQUIRED"},
            {"name": "currentness", "type": "STRING", "mode": "REQUIRED"},
        ],
        create_disposition="CREATE_IF_NEEDED",  # This will create the table if it doesn't exist
        write_disposition="WRITE_APPEND",  # This will overwrite the table if it exists
        skip_leading_rows=1,  # Skip header row
        source_format="CSV",
        field_delimiter=";",
        autodetect=False,
        gcp_conn_id="gcp_airflow",
        dag=dag,
    )
