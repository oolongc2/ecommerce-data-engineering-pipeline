from minio import Minio
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine

# ------------------
# MinIO Configuration
# ------------------
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BUCKET_NAME = "gold-zone"

# --------------------------
# PostgreSQL Configuration
# --------------------------

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "123456"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "ecommerce_db"

# --------------
# Connect MinIO
# --------------
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# ------------------
# Connect PostgreSQL
# ------------------

engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ------------------
# Files in Gold Bucket
# ------------------

datasets = {
    "user_summary.parquet": "user_summary",
    "top_products.parquet": "top_products",
    "daily_sales.parquet": "daily_sales",
    "customer_lifetime_value.parquet": "customer_lifetime_value"
}

# --------------------------
# Load Parquet -> PostgreSQL
# --------------------------
for parquet_file, table_name in datasets.items():

    print(f"\nLoading {parquet_file} into PostgreSQL table: {table_name}")

    # Get parquet object from MinIO
    response = minio_client.get_object(BUCKET_NAME, parquet_file)

    # Read parquet into pandas dataframe
    parquet_data = BytesIO(response.read())
    df = pd.read_parquet(parquet_data)

    # Load into PostgreSQL
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False
    )

    print(f"Successfully loaded {table_name}")

    response.close()
    response.release_conn()

print("\nAll datasets loaded successfully!")
