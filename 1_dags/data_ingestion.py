from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
import requests
import pandas as pd


#mock data fetching functions
def fetch_sales():
    url = "https://api.sales.com/data"
    df = pd.DataFrame(requests.get(url).json())
    df.to_csv("/tmp/sales.csv", index=False)

def fetch_trends():
    url = "https://api.trends.com/data"
    df = pd.DataFrame(requests.get(url).json())
    df.to_csv("/tmp/trends.csv", index=False)

def fetch_new_products():
    url = "https://api.products.com/data"
    df = pd.DataFrame(requests.get(url).json())
    df.to_csv("/tmp/products.csv", index=False)

dag = DAG("data_ingestion", schedule_interval="@daily", start_date=datetime(2024, 4, 1))

fetch_sales_task = PythonOperator(task_id="fetch_sales", python_callable=fetch_sales, dag=dag)
fetch_trends_task = PythonOperator(task_id="fetch_trends", python_callable=fetch_trends, dag=dag)
fetch_products_task = PythonOperator(task_id="fetch_products", python_callable=fetch_new_products, dag=dag)

upload_to_gcs = GCSToBigQueryOperator(
    task_id="upload_to_gcs",
    bucket="my-data-bucket",
    source_objects=["/tmp/sales.csv", "/tmp/trends.csv", "/tmp/products.csv"],
    destination_project_dataset_table="my_project.raw_data",
    dag=dag
)

fetch_sales_task >> upload_to_gcs
fetch_trends_task >> upload_to_gcs
fetch_products_task >> upload_to_gcs
