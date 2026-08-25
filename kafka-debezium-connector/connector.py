import requests
import time
import subprocess
import json

# Configuration
KAFKA_CONNECT_URL = 'http://localhost:8083/connectors'
CONNECTOR_NAME = 'postgres-connector'

# ---------------------------------------------------------
# Step 1: Initialize the Postgres Connector
# ---------------------------------------------------------
def create_connector():
    print("Step 1: Initializing the Postgres connector...")

    config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "host.docker.internal",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "123456",
            "database.dbname": "ecommerce_db",
            "database.server.name": "dbserver1",
            "plugin.name": "pgoutput",
            "table.include.list": "public.products,public.orders,public.users",
            "topic.prefix": "dbserver1"
        }
    }

    try:
        response = requests.post( # 'post' - not 'posts'
            KAFKA_CONNECT_URL, 
            json=config, 
            headers={"Content-Type": "application/json"}
        )
        # 409 means it already exists, 201 means created successfully
        if response.status_code in [201, 409]:
            print("Connector initialized successfully or already exists.")
        else:
            print(f"Failed to create connector. Status: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"Error connecting to Kafka Connect: {e}")

# ---------------------------------------------------------
# Step 2: Verify Connector Status
# ---------------------------------------------------------
def check_status():
    print("\nStep 2: Verifying Connector Status...")
    # Wait a brief moment to allow the connector to spin up
    time.sleep(3)

    try:
        status_url = f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}/status"
        response = requests.get(status_url)

        if response.status_code == 200:
            status_data = response.json()
            connector_state = status_data.get("connector", {}).get("state")
            print(f"Connector State: {connector_state}")
        else:
            print(f"Could not fetch status. HTTP {response.status_code}")
    except Exception as e:
        print(f"Error checking status: {e}")

# ---------------------------------------------------------
# Step 3: Verify Kafka Topics
# ---------------------------------------------------------
def list_topics():
    print("\nStep 3: Verifying Kafka Topics...")
    # Note: Removed the '-it' flag because it causes issues in non-interactive scripts
    command = [
        "docker", "exec", "kafka", 
        "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"
    ]

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        print("Available Topics:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error listing topics: {e.stderr}")

# ---------------------------------------------------------
# Step 4: Consume Real-time Data
# ---------------------------------------------------------
def consume_data():
    print("\nStep 4: Consuming Real-time Data from products table...")
    print("(Press Ctrl+C to stop consuming data)")
    
    # Listening to the products topic as an example. 
    # To listen to multiple topics, Kafka uses a regex pattern via the --include flag 
    # instead of --topic, e.g., --include "dbserver1\.public\.(products|users|orders)"
    command = [
        "docker", "exec", "kafka", 
        "kafka-console-consumer", 
        "--bootstrap-server", "localhost:9092", 
        "--topic", "dbserver1.public.products", 
        "--from-beginning"
    ]
    
    try:
        # This will block the script and stream the output continuously to your terminal
        subprocess.run(command)
    except KeyboardInterrupt:
        print("\nStopped consuming data.")

if __name__ == "__main__":
    create_connector()
    check_status()
    list_topics()
    consume_data()