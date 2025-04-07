# pip install nixtla mlforecast google-cloud-bigquery pandas airflow fastapi uvicorn

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from nixtla.mlforecast import MLForecast
from nixtla.preprocessing import DataFrame

# Define constants
BQ_PROJECT = "your-gcp-project-id"
BQ_DATASET = "your-bigquery-dataset"
BQ_SALES_TABLE = "sales_data"
BQ_TRENDS_TABLE = "trend_data"
BQ_PREDICTIONS_TABLE = "sales_predictions"

# Function to load data from BigQuery (Sales and Trends)
def load_data_from_bigquery():
    client = bigquery.Client()
    
    # Query for sales data
    sales_query = f"""
        SELECT date, product_id, sales
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_SALES_TABLE}`
    """
    sales_data = client.query(sales_query).to_dataframe()
    
    # Query for trend data (Google Trends or similar data)
    trend_query = f"""
        SELECT date, trend_score
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TRENDS_TABLE}`
    """
    trend_data = client.query(trend_query).to_dataframe()

    # Ensure the 'date' columns are in datetime format
    sales_data['date'] = pd.to_datetime(sales_data['date'])
    trend_data['date'] = pd.to_datetime(trend_data['date'])

    # Merge the sales and trend data on 'date'
    merged_data = pd.merge(sales_data, trend_data, on='date', how='left')
    
    return merged_data

# Function to preprocess data and train the model
def train_forecasting_model():
    # Load the data from BigQuery
    data = load_data_from_bigquery()

    # Prepare the data for MLForecast
    df = DataFrame(data)
    forecast = MLForecast(
        target='sales',
        static_features=['product_id'],
        time_features=['date'],
        exog_features=['trend_score']  # External features (e.g., trend data)
    )

    # Train the model
    forecast.fit(df)

    # Make predictions (e.g., for the next 30 days)
    predictions = forecast.predict(horizon=30)

    # Convert the predictions to a DataFrame
    predictions_df = pd.DataFrame(predictions, columns=['date', 'predicted_sales'])
    predictions_df['date'] = pd.to_datetime(predictions_df['date'])

    # Write the predictions back to BigQuery
    predictions_df.to_gbq(destination_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_PREDICTIONS_TABLE}", project_id=BQ_PROJECT, if_exists="replace")

    return predictions_df

'''
Data Extraction from BigQuery:

The function load_data_from_bigquery() extracts sales data and trend data from BigQuery tables (sales_data and trend_data).

These two datasets are merged based on the date column.

Data Preprocessing and Model Training:

The MLForecast library from Nixla is used to prepare the data and train the model.

External features such as trend_score (Google Trends or similar data) are used in the model.

The model is trained using the fit() function, and then predictions are made for the next 30 days.

Store Predictions Back into BigQuery:

The predictions are written to the BigQuery table sales_predictions.
'''
# Define the Airflow DAG and schedule
with DAG('train_nixla_forecasting_model', start_date=datetime(2025, 4, 1), schedule_interval='@daily') as dag:
    train_model_task = PythonOperator(
        task_id='train_model',
        python_callable=train_forecasting_model
    )

train_model_task
