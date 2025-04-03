from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)


def difference_time(timestamp):
    now = datetime.now().timestamp()
    difference = (now - int(float(timestamp))) / 3600
    if int(float(difference)) <= 0:
        return "0"
    else:
        return str(int(float(difference)))


def ingest_xtrends(**context):
    import requests
    from bs4 import BeautifulSoup
    import json

    response = {}
    web_response = requests.get(
        "https://trends24.in/france", headers={"user-agent": "Mozilla/5.0"}
    )
    content_bs = BeautifulSoup(web_response.content, "html.parser")
    print(content_bs)
    for div in content_bs.find_all("div", attrs={"class": "list-container"}):
        timestamp = div.find("h3", attrs={"class", "title"})
        if timestamp != None:
            timestamp = timestamp["data-timestamp"]
            dif = difference_time(timestamp)
            ol = div.find("ol", attrs={"class": "trend-card__list"})
            temp = {"trend": []}
            for li in ol.find_all("li"):
                tn = li.find("span", attrs={"class": "trend-name"})
                trend_name = tn.find("a", attrs={"class": "trend-link"}).text
                trend_count = tn.find("span", attrs={"class": "tweet-count"})[
                    "data-count"
                ]
                temp["trend"].append({"name": trend_name, "count": trend_count})
            response[dif] = temp
    with open("/tmp/x_trends.json", "w", encoding="utf-8") as f:
        json.dump(response, f, indent=2, ensure_ascii=False)
    return "/tmp/x_trends.json"


with DAG(
    dag_id="dag_xtrends",
    start_date=datetime(2025, 3, 24),
    default_args={
        "retries": 2,
        "email_on_failure": False,
        "start_date": datetime(2025, 4, 2),
    },
    catchup=False,
    schedule_interval="@daily",
) as dag:

    xtrends_task = PythonOperator(
        task_id="xtrends",
        python_callable=ingest_xtrends,
        dag=dag
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/tmp/x_trends.json",
        dst="x_trends.json",
        bucket="trendflow-455409-trendflow-bucket",
        gcp_conn_id="gcp_airflow",
    )

    xtrends_task >> upload_to_gcs_task
