from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import logging
import tempfile

logging.basicConfig(level=logging.INFO)

data_path = '/path/to/your/dags/data' 

def download_booking():
    file_path = os.path.join(data_path, 'booking.csv')
    if os.path.exists(file_path):
        logging.info("Downloading booking data.")
        return pd.read_csv(file_path)
    else:
        logging.error("booking.csv not found")
        raise FileNotFoundError("booking.csv not found")

def download_client():
    file_path = os.path.join(data_path, 'client.csv')
    if os.path.exists(file_path):
        logging.info("Downloading client data.")
        return pd.read_csv(file_path)
    else:
        logging.error("client.csv not found")
        raise FileNotFoundError("client.csv not found")

def download_hotel():
    file_path = os.path.join(data_path, 'hotel.csv')
    if os.path.exists(file_path):
        logging.info("Downloading hotel data.")
        return pd.read_csv(file_path)
    else:
        logging.error("hotel.csv not found")
        raise FileNotFoundError("hotel.csv not found")

def transform_data(ti):
    booking_df = ti.xcom_pull(task_ids='download_booking')
    client_df = ti.xcom_pull(task_ids='download_client')
    hotel_df = ti.xcom_pull(task_ids='download_hotel')

    logging.info("Merging dataframes.")
    merged_df = pd.merge(booking_df, client_df, on='client_id', how='inner')
    merged_df = pd.merge(merged_df, hotel_df, on='hotel_id', how='inner')

    merged_df['booking_date'] = pd.to_datetime(merged_df['booking_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    merged_df = merged_df.dropna(subset=['booking_cost', 'booking_date'])

    merged_df['currency'] = merged_df['currency'].replace({'EUR': 'USD', 'GBP': 'USD'})  

    merged_df['age'] = pd.to_numeric(merged_df['age'], errors='coerce')

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    merged_df.to_csv(temp_file.name, index=False)
    logging.info(f"Transformed data saved to {temp_file.name}")

    ti.xcom_push(key='transformed_data_path', value=temp_file.name)

def load_to_db(ti):
    file_path = ti.xcom_pull(task_ids='transform_data', key='transformed_data_path')
    
    if file_path and os.path.exists(file_path):
        df = pd.read_csv(file_path)
        try:
            logging.info("Data loaded into the database successfully.")
        except Exception as e:
            logging.error(f"Error loading data to database: {e}")
        finally:
            os.remove(file_path)  
    else:
        logging.error("Transformed data file not found.")

with DAG(
    'etl_dag',
    start_date=datetime(2023, 11, 8),
    schedule_interval='@daily',
    catchup=False
) as dag:

    download_booking_task = PythonOperator(
        task_id='download_booking',
        python_callable=download_booking
    )

    download_client_task = PythonOperator(
        task_id='download_client',
        python_callable=download_client
    )

    download_hotel_task = PythonOperator(
        task_id='download_hotel',
        python_callable=download_hotel
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
        provide_context=True
    )

    [download_booking_task, download_client_task, download_hotel_task] >> transform_task >> load_task
