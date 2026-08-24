from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
from io import BytesIO
import pandas as pd
import os # Added for file cleanup

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
    
    error_count = 0 # Track errors to fail the Airflow task later

    for obj in objects:
        if not obj.object_name.endswith(".parquet"):
            continue
            
        print(f"Found: {obj.object_name}")
        local_path = f"/tmp/clean_{obj.object_name.split('/')[-1]}"

        try:
            # 1. Get object
            response = client.get_object(
                "bronze-zone", 
                obj.object_name
            )
            data = BytesIO(response.read())
            response.close()
            response.release_conn()

            # 2. Transform
            df = pd.read_parquet(data)
            df = df.dropna()

            # 3. Save locally
            df.to_parquet(local_path, index=False)

            # 4. Upload to Silver
            silver_object_name = f"clean_{obj.object_name.split('/')[-1]}"
            client.fput_object("silver-zone", silver_object_name, local_path)
            print(f"Uploaded: {silver_object_name}")

        except Exception as e:
            print(f"Error processing {obj.object_name}: {e}")
            error_count += 1
            
        finally:
            # 5. CLEANUP: Always remove the local file to prevent Docker storage crashes
            if os.path.exists(local_path):
                os.remove(local_path)

    # 6. FAIL THE DAG IF NECESSARY
    if error_count > 0:
        raise RuntimeError(f"Task completed with {error_count} file processing errors. Check logs.")

with DAG(
    dag_id="bronze_to_silver",
    start_date=datetime(2024, 1, 1),
    #schedule_interval="@daily", # Remove this parameter because of unexpected keyword argument error
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="process_data",
        python_callable=process_data
    )