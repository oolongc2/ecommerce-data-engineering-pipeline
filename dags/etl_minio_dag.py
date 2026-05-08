from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
from io import BytesIO
import pandas as pd


def process_data():

    # Connect to MinIO
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    # Create silver bucket if not exists
    if not client.bucket_exists("silver-zone"):
        client.make_bucket("silver-zone")

    # List all parquet files in bronze-zone
    objects = client.list_objects(
        "bronze-zone",
        recursive=True
    )

    for obj in objects:

        print(f"Found: {obj.object_name}")

        # Process only parquet files
        if obj.object_name.endswith(".parquet"):

            try:

                # Get object from MinIO
                response = client.get_object(
                    "bronze-zone",
                    obj.object_name
                )

                # Convert stream to BytesIO
                data = BytesIO(response.read())

                # Close MinIO connection
                response.close()
                response.release_conn()

                # Read parquet file
                df = pd.read_parquet(data)

                print(f"Loaded: {obj.object_name}")
                print(df.head())

                # Example transformation
                df = df.dropna()

                # Save cleaned file locally
                local_path = f"/tmp/clean_{obj.object_name.split('/')[-1]}"

                df.to_parquet(
                    local_path,
                    index=False
                )

                # Upload to silver-zone
                client.fput_object(
                    "silver-zone",
                    f"clean_{obj.object_name.split('/')[-1]}",
                    local_path
                )

                print(f"Uploaded: clean_{obj.object_name.split('/')[-1]}")

            except Exception as e:
                print(f"Error processing {obj.object_name}")
                print(e)


with DAG(
    dag_id="bronze_to_silver",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="process_data",
        python_callable=process_data
    )