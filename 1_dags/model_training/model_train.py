import os
import pickle
from datetime import datetime
from flask.cli import load_dotenv
import pandas as pd
import numpy as np
from google.cloud import bigquery
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from dotenv import load_dotenv



# --- Configuration ---
load_dotenv(".envrc")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_NAME = os.getenv("DATASET_NAME")
GCS_BUCKET = os.getenv("GCS_BUCKET")
TREND_TABLE = f"{PROJECT_ID}.{DATASET_NAME}.trend_data"
PRODUCT_TABLE = f"{PROJECT_ID}.{DATASET_NAME}.new_products"

# Initialize BigQuery client
# Make sure you are authenticated (e.g., using gcloud auth application-default login)
client = bigquery.Client(project=PROJECT_ID)

def fetch_data(query):
    """Executes a BigQuery query and returns a pandas DataFrame."""
    try:
        print(f"Executing query:\n{query}\n")
        query_job = client.query(query)
        results = query_job.result()  # Waits for the job to complete
        df = results.to_dataframe()
        print(f"Fetched {len(df)} rows.")
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame() # Return empty DataFrame on error
    

def train_and_save_model():
    
    # Fetch Trends Data (Keywords)
    trends_query = f"""
        SELECT DISTINCT
            trend_id,
            keyword
        FROM `{TREND_TABLE}`
        WHERE keyword IS NOT NULL AND TRIM(keyword) != ''
    """
    trends_df = fetch_data(trends_query)
    # trends_df.head()
       
    # Fetch Products Data (Name and Category)
    products_query = f"""
        SELECT DISTINCT
            product_id,
            product_name,
            category
        FROM `{PRODUCT_TABLE}`
        WHERE product_name IS NOT NULL AND TRIM(product_name) != ''
    """
    products_df = fetch_data(products_query)

    # Combine product name and category into a single text field for vectorization
    # Handle potential missing categories gracefully
    products_df['product_text'] = products_df['product_name'] + ' ' + products_df['category'].fillna('')
    products_df = products_df[['product_id', 'product_text']].dropna(subset=['product_text'])
    # products_df.head()
    
    # Initialize TF-IDF Vectorizer
    # Using stop_words='english' to remove common English words
    vectorizer = TfidfVectorizer(stop_words='english', lowercase=True)

    # Fit and transform the product descriptions
    product_vectors = vectorizer.fit_transform(products_df['product_text'])

    # Transform the trend keywords using the same vectorizer
    # Note: We only transform trends, don't fit again, to use the product vocabulary
    trend_vectors = vectorizer.transform(trends_df['keyword'])

    print(f"Product Vectors Shape: {product_vectors.shape}")
    print(f"Trend Vectors Shape: {trend_vectors.shape}")
        
    # Calculate cosine similarity between all trends and all products
    # Resulting matrix shape: (n_trends, n_products)
    similarity_matrix = cosine_similarity(trend_vectors, product_vectors)

    print(f"Similarity Matrix Shape: {similarity_matrix.shape}")
        
    # Get the current date and time
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Generate filenames with the current date and time
    model_filename = f"tfidf_model_{current_datetime}.pkl"
    similarity_filename = f"similarity_matrix_{current_datetime}.pkl"

    # Save the model and similarity matrix
    with open(model_filename, "wb") as f:
        pickle.dump(vectorizer, f)
    with open(similarity_filename, "wb") as f:
        pickle.dump(similarity_matrix, f)

    print(f"Model saved as: {model_filename}")
    print(f"Similarity matrix saved as: {similarity_filename}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    "train_tfidf_model",
    default_args=default_args,
    schedule_interval="0 8 * * *",  # Tous les jours à 08h00
    catchup=False,
)

train_task = PythonOperator(
    task_id="train_model",
    python_callable=train_and_save_model,
    dag=dag,
)

upload_model_task = LocalFilesystemToGCSOperator(
    task_id="upload_model",
    src="tfidf_model.pkl",
    dst="models/tfidf_model.pkl",
    bucket="votre-bucket-gcs",
    dag=dag,
)

upload_similarity_task = LocalFilesystemToGCSOperator(
    task_id="upload_similarity",
    src="similarity_matrix.pkl",
    dst="models/similarity_matrix.pkl",
    bucket="votre-bucket-gcs",
    dag=dag,
)

train_task >> [upload_model_task, upload_similarity_task]
