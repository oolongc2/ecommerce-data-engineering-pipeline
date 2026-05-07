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
    # List all objects in bronze-zone bucket (products, users, orders)
    objects = client.list_objects("bronze-zone", recursive=True)

    for obj in objects:
        print("Found:", obj.object_name)
        
        if obj.object_name.endswith(".parquet"):

            df = pd.read_parquet(client.get_object("bronze-zone", obj.object_name))

            content = df.read()

            print(f"Loaded: {obj.object_name}")
            print(f"Size: {len(content)} bytes")

            break

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