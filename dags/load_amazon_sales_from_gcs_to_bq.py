from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from google.cloud import bigquery

# GCS CSV file path
GCS_PATH = "gs://trendflow-455409-trendflow-bucket/Amazon Sale Report.csv"

# BigQuery settings
PROJECT_ID = "trendflow-455409"
DATASET_ID = "trendflow"
TABLE_ID = "sales"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 31),
    "retries": 1,
}

def load_csv_from_gcs_to_bq():
    print("📥 Reading CSV file from GCS...")
    # Read the CSV from GCS
    df = pd.read_csv(GCS_PATH, storage_options={"token": "default"})

    print("🧼 Normalizing column names...")
    # Rename columns to match BigQuery schema (replace spaces with underscores)
    df.rename(columns={
        "Sales Channel ": "Sales_Channel",
        "Sales Channel": "Sales_Channel",
        "Date": "Date",
        "ASIN": "ASIN",
        "Qty": "Qty",
        "Category": "Category"
    }, inplace=True)

    # Filter only expected columns
    expected_columns = ["Date", "ASIN", "Qty", "Sales_Channel", "Category"]
    df = df[expected_columns]

    print("🔁 Converting data types...")
    # Convert types to match BigQuery schema
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce")

    print("🚀 Uploading to BigQuery...")
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Load to BigQuery in append mode
    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )
    )
    job.result()  # Wait for job to complete
    print(f"✅ Loaded {len(df)} rows into {table_ref}")

# Define DAG
with DAG("load_amazon_sales_from_gcs_to_bq",
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    load_task = PythonOperator(
        task_id="load_csv_to_bigquery",
        python_callable=load_csv_from_gcs_to_bq
    )