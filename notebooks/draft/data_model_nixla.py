import os
import requests
from datetime import datetime
import logging

import holidays
import numpy as np
import pandas as pd
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
# Consider using Airflow Variables or Connections for sensitive info
# from airflow.models import Variable 

# --- Configuration ---
# Nixtla API Configuration (Replace with secure retrieval method)
NIXTLA_TOKEN = os.getenv("NIXTLA_TOKEN", "YOUR_NIXTLA_API_TOKEN") # Use environment variable or Airflow Variable
INPUT_SIZE_ENDPOINT = os.getenv("NIXTLA_INPUT_SIZE_ENDPOINT", "YOUR_INPUT_SIZE_ENDPOINT")
FORECAST_ENDPOINT = os.getenv("NIXTLA_FORECAST_ENDPOINT", "YOUR_FORECAST_ENDPOINT")

# BigQuery Configuration (Replace with secure retrieval method)
BQ_PROJECT = os.getenv("GCP_PROJECT", "your-gcp-project-id") # Use environment variable or Airflow Variable
BQ_DATASET = os.getenv("BQ_DATASET", "your-bigquery-dataset")
BQ_SALES_TABLE = "udata_sales_history"
BQ_TRENDS_TABLE = "udata_trend"
BQ_PRODUCTS_TABLE = "udata_product_catalog"
BQ_PREDICTIONS_TABLE = "sales_predictions_timegpt" # New table for TimeGPT results

# Forecasting Parameters
FORECAST_HORIZON = 30
FREQ = 'D' # Assuming Daily frequency, adjust as needed
FINETUNE_STEPS = 0 # Default to zero-shot
PREDICTION_LEVEL = 90 # For prediction intervals
ADD_DEFAULT_CAL_VARS = True # Add default day/week/month features
HOLIDAY_COUNTRIES = "US" # Example: Add US holidays, comma-separated if multiple

# --- Helper Functions (Adapted from Streamlit Sample) ---

def generate_calendar_features(unique_id_df, freq, horizon, add_default, countries_str):
    """
    Generates calendar features for a DataFrame belonging to a single unique_id.
    """
    start_date = unique_id_df['ds'].min()
    # Generate date range including history and future horizon
    # Determine periods needed: historical length + future horizon
    history_len = len(unique_id_df)
    date_range = pd.date_range(start=start_date, periods=history_len + horizon, freq=freq)
    cal_df = pd.DataFrame({'ds': date_range})
    
    # Add holiday features
    if countries_str:
        countries = countries_str.split(',')
        for country in countries:
            try:
                country_holidays = holidays.CountryHoliday(country.strip(), years=cal_df['ds'].dt.year.unique().tolist())
                cal_df[f'holiday_{country.strip()}'] = cal_df['ds'].apply(lambda date: 1 if date in country_holidays else 0)
            except KeyError:
                logging.warning(f"Could not find holiday calendar for country: {country.strip()}")

    # Add default calendar features
    if add_default:
        # Frequencies containing day information
        day_freqs = ["D", "B", "H", "T", "S", "L", "U", "N"]
        # Frequencies containing week or month information
        week_month_freqs = ["W", "M", "Q", "A", "Y"]
        
        if any(freq.startswith(day_freq) for day_freq in day_freqs):
            cal_df['day_of_week'] = cal_df['ds'].dt.dayofweek
            cal_df['is_weekend'] = cal_df['day_of_week'].isin([5, 6]).astype(int)
            cal_df['day_of_year'] = cal_df['ds'].dt.dayofyear
            cal_df['week_of_year'] = cal_df['ds'].dt.isocalendar().week.astype(int) # Use isocalendar().week
            cal_df['month'] = cal_df['ds'].dt.month
            cal_df['quarter'] = cal_df['ds'].dt.quarter
            # Consider adding interactions or cyclical features if needed

        if any(freq.startswith(week_month_freq) for week_month_freq in week_month_freqs):
            cal_df['month'] = cal_df['ds'].dt.month
            cal_df['quarter'] = cal_df['ds'].dt.quarter
            
    # Drop original date column before returning features
    return cal_df.drop(columns=['ds'])


def call_nixtla_api(y_df, fh, freq, level, finetune_steps, x_df=None):
    """
    Calls the Nixtla TimeGPT forecast API.
    y_df: DataFrame with columns ['unique_id', 'ds', 'y']
    x_df: Optional DataFrame with exogenous variables, including ['unique_id', 'ds']
          Must cover the historical period AND the forecast horizon.
    """
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {NIXTLA_TOKEN}"
    }

    # --- Get Required Input Size ---
    try:
        input_size_response = requests.post(
            INPUT_SIZE_ENDPOINT,
            json={"freq": freq},
            headers=headers,
        )
        input_size_response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        input_size = input_size_response.json().get('data', 1) # Default to 1 if not found? Check API docs
        logging.info(f"Nixtla API requires input size: {input_size}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting input size from Nixtla API: {e}")
        raise
    except Exception as e:
         logging.error(f"Error processing input size response: {e}")
         raise

    # --- Prepare Payload ---
    # Ensure data types are correct and trim history
    y_df['ds'] = pd.to_datetime(y_df['ds']).astype(str)
    y_payload_df = y_df.groupby('unique_id').tail(input_size) # Keep only required history
    y_payload = y_payload_df.to_dict(orient='split', index=False)

    x_payload = None
    if x_df is not None and not x_df.empty:
        x_df['ds'] = pd.to_datetime(x_df['ds']).astype(str)
        # Ensure x_df covers history tail + future horizon
        required_x_len = input_size + fh
        x_payload_df = x_df.groupby('unique_id').tail(required_x_len) 
        
        # Check if we have enough future exogenous data
        if len(x_payload_df) < len(y_payload_df.unique_id.unique()) * required_x_len:
             logging.warning("Exogenous data might not cover the full required history + horizon.")
             # Handle this case: error out, fill missing, etc. - For now, proceed.

        x_payload = x_payload_df.to_dict(orient='split', index=False)

    payload = {
        "y": y_payload,
        "x": x_payload,
        "fh": fh,
        "level": [level] if level else None,
        "finetune_steps": finetune_steps,
        "clean_ex_first": True, # Default from sample
        "freq": freq,
    }

    # --- Call Forecast API ---
    logging.info(f"Calling Nixtla Forecast API for {len(y_df['unique_id'].unique())} series...")
    try:
        response = requests.post(
            FORECAST_ENDPOINT,
            json=payload,
            headers=headers
        )
        response.raise_for_status()
        response_data = response.json().get('data', {})
        
        forecast_df = pd.DataFrame(**response_data.get('forecast', {}))
        weights = response_data.get('weights_x') # Weights for exogenous features
        
        if forecast_df.empty:
             logging.warning("Received empty forecast DataFrame from Nixtla API.")
             return pd.DataFrame(), None # Return empty df

        logging.info(f"Received forecast for {len(forecast_df['unique_id'].unique())} series.")
        return forecast_df, weights

    except requests.exceptions.RequestException as e:
        logging.error(f"Error calling Nixtla Forecast API: {e}")
        # Log response body if possible and safe
        try:
            logging.error(f"Response body: {response.text}")
        except Exception:
            pass # Ignore if response object doesn't exist or text fails
        raise
    except Exception as e:
         logging.error(f"Error processing forecast response: {e}")
         raise


# --- Main Workflow Function ---

def forecast_with_timegpt_workflow():
    """
    Orchestrates loading data, preparing features, calling TimeGPT API, and saving results.
    """
    client = bigquery.Client(project=BQ_PROJECT)
    logging.info("Starting TimeGPT forecasting workflow...")

    # 1. Load Base Sales Data
    logging.info(f"Loading sales data from {BQ_DATASET}.{BQ_SALES_TABLE}...")
    sales_query = f"""
        SELECT date, product_id, sales
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_SALES_TABLE}`
        ORDER BY product_id, date
    """
    try:
        sales_data = client.query(sales_query).to_dataframe()
        if sales_data.empty:
            logging.error("No sales data loaded. Aborting.")
            return
        sales_data['ds'] = pd.to_datetime(sales_data['date'])
        sales_data = sales_data.rename(columns={'product_id': 'unique_id', 'sales': 'y'})
        logging.info(f"Loaded {len(sales_data)} sales records for {sales_data['unique_id'].nunique()} products.")
    except Exception as e:
        logging.error(f"Failed to load sales data: {e}")
        raise

    # 2. Prepare Exogenous Features (Trends + Calendar)
    all_exog_df = pd.DataFrame()

    # Load Trend Data (Example: Global daily trend)
    logging.info(f"Loading trend data from {BQ_DATASET}.{BQ_TRENDS_TABLE}...")
    trend_query = f"""
        SELECT date, AVG(trend_score) as trend_score 
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TRENDS_TABLE}`
        GROUP BY date
        ORDER BY date
    """
    try:
        trend_data = client.query(trend_query).to_dataframe()
        if not trend_data.empty:
            trend_data['ds'] = pd.to_datetime(trend_data['date'])
            logging.info(f"Loaded {len(trend_data)} trend records.")
            # We need trend data for the forecast horizon too. Simple forward fill for example.
            # A better approach would be to forecast trends separately.
            if not trend_data.empty:
                 last_date = trend_data['ds'].max()
                 future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=FORECAST_HORIZON, freq=FREQ)
                 future_trends = pd.DataFrame({'ds': future_dates})
                 # Forward fill the last known trend score
                 future_trends['trend_score'] = trend_data['trend_score'].iloc[-1] 
                 trend_data = pd.concat([trend_data.drop(columns=['date']), future_trends], ignore_index=True)
            all_exog_df = trend_data
        else:
             logging.warning("No trend data loaded.")
    except Exception as e:
        logging.error(f"Failed to load or process trend data: {e}")
        # Decide if this is critical - perhaps proceed without trends? For now, raise.
        raise

    # Generate Calendar Features for each unique_id
    logging.info("Generating calendar features...")
    calendar_features_list = []
    grouped_sales = sales_data.groupby('unique_id')
    for name, group in grouped_sales:
        # Generate features covering history + horizon
        cal_feats = generate_calendar_features(group[['ds']], FREQ, FORECAST_HORIZON, ADD_DEFAULT_CAL_VARS, HOLIDAY_COUNTRIES)
        # Need to align dates with the full range (history + horizon)
        start_date = group['ds'].min()
        full_date_range = pd.date_range(start=start_date, periods=len(group) + FORECAST_HORIZON, freq=FREQ)
        cal_feats['ds'] = full_date_range
        cal_feats['unique_id'] = name
        calendar_features_list.append(cal_feats)
    
    if calendar_features_list:
        calendar_df = pd.concat(calendar_features_list, ignore_index=True)
        logging.info(f"Generated calendar features, shape: {calendar_df.shape}")
        
        # Merge calendar features with trends (if available)
        if not all_exog_df.empty:
            all_exog_df = pd.merge(calendar_df, all_exog_df, on='ds', how='left')
            # Handle potential NaNs from merge (e.g., fill trend score if dates don't align perfectly)
            all_exog_df['trend_score'] = all_exog_df['trend_score'].ffill().bfill() 
        else:
            all_exog_df = calendar_df
    else:
        logging.warning("No calendar features generated.")
        # all_exog_df remains just trends, or empty if no trends either

    # Ensure no NaN in final exogenous features before sending to API
    if not all_exog_df.empty:
         all_exog_df = all_exog_df.fillna(0) # Simple fillna strategy
         logging.info(f"Final exogenous features shape: {all_exog_df.shape}")
         # Check if all_exog_df covers the required time range for all series
         # (Min date should match sales min date, max date should cover horizon)
    else:
         logging.warning("Proceeding without any exogenous features.")


    # 3. Call Nixtla API
    try:
        forecast_results, weights = call_nixtla_api(
            y_df=sales_data[['unique_id', 'ds', 'y']],
            fh=FORECAST_HORIZON,
            freq=FREQ,
            level=PREDICTION_LEVEL,
            finetune_steps=FINETUNE_STEPS,
            x_df=all_exog_df # Pass the combined exogenous features
        )
    except Exception as e:
        logging.error(f"Nixtla API call failed: {e}")
        # Depending on requirements, might want to stop the DAG run here
        raise 

    # 4. Process and Save Results
    if forecast_results.empty:
        logging.warning("No forecast results received from API. Skipping save.")
        return

    logging.info("Processing forecast results...")
    # Rename columns for clarity if needed
    forecast_results = forecast_results.rename(columns={'unique_id': 'product_id', 'ds': 'forecast_date', 'TimeGPT': 'predicted_sales'})
    # Ensure correct data types
    forecast_results['forecast_date'] = pd.to_datetime(forecast_results['forecast_date'])
    # Add timestamp
    forecast_results['prediction_timestamp'] = datetime.utcnow()
    
    # Select columns to save (adjust based on API response)
    cols_to_save = ['product_id', 'forecast_date', 'predicted_sales', 'prediction_timestamp']
    # Add prediction intervals if they exist
    lo_col = f'TimeGPT-lo-{PREDICTION_LEVEL}'
    hi_col = f'TimeGPT-hi-{PREDICTION_LEVEL}'
    if lo_col in forecast_results.columns:
        cols_to_save.append(lo_col)
        forecast_results = forecast_results.rename(columns={lo_col: 'predicted_sales_low'})
    if hi_col in forecast_results.columns:
         cols_to_save.append(hi_col)
         forecast_results = forecast_results.rename(columns={hi_col: 'predicted_sales_high'})


    logging.info(f"Writing {len(forecast_results)} predictions to BigQuery table {BQ_DATASET}.{BQ_PREDICTIONS_TABLE}...")
    try:
        forecast_results[cols_to_save].to_gbq(
            destination_table=f"{BQ_DATASET}.{BQ_PREDICTIONS_TABLE}",
            project_id=BQ_PROJECT,
            if_exists="replace" # Or 'append'
        )
        logging.info("Successfully wrote predictions to BigQuery.")
    except Exception as e:
        logging.error(f"Failed to write predictions to BigQuery: {e}")
        raise

    # Optionally save feature weights if needed
    if weights is not None and not all_exog_df.empty:
        try:
            exog_feature_names = all_exog_df.drop(columns=['unique_id', 'ds']).columns.tolist()
            weights_df = pd.DataFrame({'feature': exog_feature_names, 'weight': weights})
            logging.info(f"Exogenous Feature Weights:\n{weights_df}")
            # Save weights_df to BQ or log them
            # weights_df.to_gbq(...) 
        except Exception as e:
             logging.warning(f"Could not process or save feature weights: {e}")


# --- Airflow DAG Definition ---
with DAG(
    dag_id='forecast_with_timegpt_api',
    start_date=datetime(2025, 4, 4), # Adjust start date
    schedule_interval='@daily', # Or None, or cron expression
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    }
) as dag:
    run_timegpt_forecast_task = PythonOperator(
        task_id='run_timegpt_forecast',
        python_callable=forecast_with_timegpt_workflow
    )

# Define task dependencies if needed
# run_timegpt_forecast_task
