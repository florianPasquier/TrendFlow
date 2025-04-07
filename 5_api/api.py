from fastapi import FastAPI
from google.cloud import bigquery
import pandas as pd

app = FastAPI()

@app.get("/predictions")
def get_predictions():
    client = bigquery.Client()
    query = "SELECT * FROM `my_project.my_dataset.predictions`"
    df = client.query(query).to_dataframe()
    return df.to_dict(orient="records")

@app.get("/recommendations")
def get_recommendations():
    client = bigquery.Client()
    query = "SELECT * FROM `my_project.my_dataset.recommendations`"
    df = client.query(query).to_dataframe()
    return df.to_dict(orient="records")
