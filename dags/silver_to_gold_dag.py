from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
from io import BytesIO
import pandas as pd

def build_gold_layer():

    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    # Create gold bucket if not exists
    if not client.bucket_exists("gold-zone"):
        client.make_bucket("gold-zone")
    
    # -----------------
    # LOAD SILVER DATA
    # -----------------

    def load_parquet(bucket, object_name):

        response = client.get_object(bucket, object_name)

        data = BytesIO(response.read())

        response.close()
        response.release_conn()

        return pd.read_parquet(data)

    # Load parquet files into tables to handle data
    users_df = load_parquet(
        "silver-zone",
        "clean_users_20260507_091636_201362.parquet"
    )

    products_df = load_parquet(
        "silver-zone",
        "clean_products_20260504_192848_628904.parquet"
    )

    orders_df = load_parquet(
        "silver-zone",
        "clean_orders_20260507_091636_254508.parquet"
    )

    # -----------------------------
    # GOLD 1 - USER ORDER SUMMARY
    # -----------------------------

    user_summary = orders_df.groupby("user_id").agg(
        total_orders=("order_id", "count"),
        total_quantity=("quantity", "sum"),
        total_spent=("total_price", "sum")
    ).reset_index()

    user_summary = user_summary.merge(
        users_df, 
        on="user_id", 
        how="left"
    )
    
    print("Built user_summary successfully!!!")

    # ---------------------
    # GOLD 2 - TOP PRODUCTS
    # ---------------------

    top_products = orders_df.groupby("product_id").agg(
        total_quantity_sold=("quantity", "sum"),
        total_revenue=("total_price", "sum")
    ).reset_index()

    top_products = top_products.merge(
        products_df,
        on="product_id",
        how="left"
    )

    top_products = top_products.sort_values(
        by="total_revenue", 
        ascending=False
    )

    print("Built top_products successfully!!!")

    # ----------------------
    # GOLD 3 - DAILY SALES
    # ----------------------

    orders_df["order_date"] = pd.to_datetime(
        orders_df["order_created_at"]
    )

    daily_sales = orders_df.groupby("order_date").agg(
        total_orders=("order_id", "count"),
        total_revenue=("total_price", "sum"),
        total_quantity=("quantity", "sum")
    ).reset_index()

    print("Built daily_sales successfully!!!")

    # --------------------------------
    # GOLD 4 - CUSTOMER LIFETIME VALUE
    # --------------------------------

    clv = orders_df.groupby("user_id").agg(
        lifetime_orders=("order_id", "count"),
        lifetime_revenue=("total_price", "sum")
    ).reset_index()

    clv = clv.merge(
        users_df,
        on="user_id",
        how="left"
    )

    print("Built clv successfully!!!")

    # ----------------------
    # SAVE GOLD DATASETS
    # ----------------------

    datasets = {
        "user_summary.parquet": user_summary,
        "top_products.parquet": top_products,
        "daily_sales.parquet": daily_sales,
        "customer_lifetime_value.parquet": clv
    }

    for filename, dataframe in datasets.items():

        local_path = f"/tmp/{filename}"

        dataframe.to_parquet(
            local_path,
            index=False
        )

        client.fput_object(
            "gold-zone",
            filename,
            local_path
        )

        print(f"Uploaded {filename} to gold-zone successfully!!!")


with DAG(
    dag_id="silver_to_gold",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    build_gold_task = PythonOperator(
        task_id="build_gold_layer",
        python_callable=build_gold_layer
    )