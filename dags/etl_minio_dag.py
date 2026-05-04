from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import pandas as pd

def process_data():
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    # Read file from bronze-zone MinIO bucket
    obj = client.get_object("bronze-zone", "products_20260504_192848_628904.parquet")

    df = pd.read_parquet(obj)

    # Process data (example: drop rows with missing values)
    df = df.dropna()

    # Save processed data to a new Parquet file
    df.to_parquet("/tmp/clean.parquet", index=False)

    client.fput_object(
        "silver-zone",
        "products_clean.parquet",
        "/tmp/clean.parquet"
    )

with DAG("bronze_to_silver",
         start_date=datetime(2024,1,1),
         schedule_interval="@daily",
         catchup=False) as dag:

    task = PythonOperator(
        task_id="process_data",
        python_callable=process_data
    )