import json
import io
from datetime import datetime
import pandas as pd
from confluent_kafka import Consumer, KafkaException
from minio import Minio

# ===================
# 1. Kafka configuration
# ===================
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPICS = [
    'dbserver1.public.products',
    'dbserver1.public.orders',
    'dbserver1.public.users'
]
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'minioadmin' # Replace with your MinIO access key
MINIO_SECRET_KEY = 'minioadmin' # Replace with your MinIO secret key
BUCKET_NAME = 'bronze-zone'
BATCH_SIZE = 1000

# Initialize buffer for temporary data storage
data_buffers = {
    'products': [],
    'orders': [],
    'users': []
}

# ===================
# 2. Initialize Connections
# ===================
# Initialize MinIO Client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Initialize Kafka Consumer
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'ecommerce_data_lake_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False # Disable auto-commit to manage offsets manually
}

consumer = Consumer(conf)
consumer.subscribe(KAFKA_TOPICS)

# ===================
# 3. Processing Functions and Update Data
# ===================
def flush_buffer_to_minio(table_name):
    """Flush the buffer to MinIO as a Parquet file and push to MinIO."""
    if not data_buffers[table_name]:
        return
    
    # Convert buffer to DataFrame
    df = pd.DataFrame(data_buffers[table_name])

    # Create a unique file name based on timestamp
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    file_name = f"{table_name}/{table_name}_{timestamp_str}.parquet"

    # Write DataFrame to a Parquet file in RAM memory
    parquet_bytes = df.to_parquet(engine='pyarrow', index=False)
    parquet_buffer = io.BytesIO(parquet_bytes)

    # Upload the Parquet file to MinIO
    minio_client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=file_name,
        data=parquet_buffer,
        length=len(parquet_bytes),
        content_type='application/octet-stream'
    )

    print(f"Flushed {len(df)} records onto MinIO: s3://{BUCKET_NAME}/{file_name}")

    # Clear the buffer after flushing
    data_buffers[table_name].clear()

# ===================
# 4. Main Loop to Consume Kafka Messages
# ===================
print("Starting Kafka consumer...")

try:
    while True:
        msg = consumer.poll(timeout=1.0) # Waiting message for 1 second

        if msg is None:
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue
        
        # Get table name from topic
        topic_name = msg.topic()
        table_name = topic_name.split('.')[-1] # Extract table name from topic

        # Parse the message value (assuming it's JSON)
        try:
            msg_value = json.loads(msg.value().decode('utf-8'))
        except Exception as e:
            print(f"Error parsing JSON: {e}")
            continue

        payload = msg_value.get('payload')
        if not payload:
            continue

        # Get the operation type (c, u, r, d)
        op = payload.get('op')
        
        # c = create/insert, u = update, r = read (first snapshot)
        if op in ['c', 'u', 'r']:
            record_data = payload.get('after')
        # d = delete
        elif op == 'd':
            record_data = payload.get('before')
        else:
            continue

        if record_data:
            # Assign CDC metadata
            record_data['_cdc_op'] = op
            record_data['_cdc_timestamp'] = datetime.now().isoformat()
            
            # Add record to the buffer
            data_buffers[table_name].append(record_data)
            
            # Check if buffer has reached the batch size, if yes, flush to MinIO
            if len(data_buffers[table_name]) >= BATCH_SIZE:
                flush_buffer_to_minio(table_name)
                # Commit the Kafka offset after processing the batch
                consumer.commit()

except KeyboardInterrupt:
    print("\nReceived shutdown signal, exiting...")
finally:
    # Clean up any remaining data in buffers before closing
    print("Cleaning up...")
    for table in data_buffers.keys():
        flush_buffer_to_minio(table)
    
    consumer.commit() # Final commit before closing
    consumer.close()
    print("Consumer closed.")