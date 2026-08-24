from minio import Minio

MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'minioadmin' # Replace with your MinIO access key
MINIO_SECRET_KEY = 'minioadmin' # Replace with your MinIO secret key
BUCKET_NAME = 'bronze-zone'

# Initialize MinIO Client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

bucket = ["bronze-zone", "silver-zone", "gold-zone"]

for bucket_name in bucket:

    # Check whether the bucket exists
    if not minio_client.bucket_exists(bucket_name):
        print(f"Bucket '{bucket_name}' does not exist.")
        continue

    # Get all objects in the bucket
    objects = minio_client.list_objects(
        bucket_name, 
        recursive=True
    )

    # Delete objects
    for obj in objects:
        minio_client.remove_object(
            bucket_name, 
            obj.object_name
        )
        print(f"Deleted object '{obj.object_name}' from bucket '{bucket_name}'.")

    # Delete empty bucket
    minio_client.remove_bucket(bucket_name)

    print(f"Deleted bucket '{bucket_name}' and all its contents.")

