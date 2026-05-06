# ecommerce-data-engineering-pipeline
🚀 Real-time E-commerce Data Engineering Pipeline

## 📌 Project Overview
This small project demonstrates a real-time data pipeline using some modern and popular Data Engineering tools.

The system simulates an e-commerce environment where users continuously place orders, and the data flow through a complete pipeline from ingestion to analytics and visualization.

## 🏗️ Architechture
PostgreSQL → Debezium → Kafka → Python Consumer → MinIO (Data Lake) → Airflow → dbt → Data Warehouse → Dashboard

## ⚙️ Technologies
- Python – Data generation & processing
- PostgreSQL – Source database
- Debezium – Change Data Capture (CDC)
- Apache Kafka – Real-time streaming
- MinIO – Data lake storage
- Apache Airflow – Workflow orchestration
- dbt – Data transformation

## 📋 Prerequisites
Before you begin, ensure you have met the following requirements:
* [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running.
* Windows PowerShell (for running the setup scripts).
* A SQL client like pgAdmin4 (optional, for manual database inspection).

---

## 🏃‍♂️ How to Run the Project

### Step 1: Start the Docker Infrastructure
Start all necessary services (PostgreSQL, Zookeeper, Kafka, Kafka Connect) in detached mode:
```powershell
docker-compose up -d
```

### Step 2: Initialize the Postgres Connector
Open PowerShell and run the following script to configure and create the Debezium Postgres Connector via the Kafka Connect REST API:
```powershell
$body = @{
  name = "postgres-connector"
  config = @{
    "connector.class" = "io.debezium.connector.postgresql.PostgresConnector"
    "database.hostname" = "host.docker.internal"
    "database.port" = "5432"
    "database.user" = "postgres"
    "database.password" = "123456"
    "database.dbname" = "ecommerce_db"
    "database.server.name" = "dbserver1"
    "plugin.name" = "pgoutput"
    "table.include.list" = "public.products"
    "topic.prefix" = "dbserver1"
  }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Uri "http://localhost:8083/connectors" `
-Method Post `
-ContentType "application/json" `
-Body $body
```

### Step 3: Verify Connector Status
```powershell
Invoke-RestMethod http://localhost:8083/connectors/postgres-connector/status
```
(Optional) If you encounter an error or need to update the configuration, you can delete the connector using:
```powershell
Invoke-RestMethod -Uri http://localhost:8083/connectors/postgres-connector -Method Delete
```

### Step 4: Verify Kafka Topics
Check if Debezium has successfully automatically created the topic for the products table. Run this command to list all topics in Kafka:
```powershell
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Step 5: Consume Real-time Data
```powershell
docker exec -it kafka kafka-console-consumer `
--bootstrap-server localhost:9092 `
--topic dbserver1.public.products `
--from-beginning
```

### 🧹 Teardown
To stop the pipeline and remove the containers, networks, and volumes (this will wipe the database data), run:
```
docker-compose down -v
```

## 📦 MinIO Setup (Data Lake - Bronze Layer)

This section describes how to set up **MinIO** as object storage and stream data from Kafka into a Bronze layer in Parquet format.

---

### 🚀 1. Create Consumer Script

First, create a Python script to consume data from Kafka and push it to MinIO:

```bash
kafka_to_minio_consumer.py
```

### 🌐 2. Access MinIO Web UI

Open the MinIO Console in your browser:

```bash
http://localhost:9001
```

### 🪣 3. Create Bucket (Bronze Layer)

MinIO uses buckets to organize data (similar to folders in a filesystem).

Create a bucket named: bronze-zone

### 📚 4. Install Required Python Libraries

Install the necessary dependencies:

```bash
pip install confluent-kafka pandas pyarrow minio
```

### 🔍 5. Verify Kafka Data

Before consuming data into MinIO, verify that Kafka is producing data correctly:

```bash
docker exec -it kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic dbserver1.public.products \
--from-beginning
```

### ▶️ 6. Run the Consumer Script

Execute the Python script to start ingesting data into MinIO

```bash
python kafka_to_minio_consumer.py
```

### ✅ 7. Validate Data in MinIO

Go back to MinIO Web UI to see a folder: products/ . Inside it, Parquet files: *.parquet

## 🔄 Apache Airflow Setup (Workflow Orchestration)

This section describes how to set up **Apache Airflow** to orchestrate data pipelines in the project.

Airflow is used to:
- Schedule and monitor workflows (DAGs)
- Automate data ingestion and processing tasks
- Manage dependencies between different pipeline stages

---

### 🚀 1. Install Required Dependencies

To enable integration with cloud/object storage services (such as MinIO via S3 API), install the Amazon provider package:

```bash
pip install apache-airflow-providers-amazon
```

### 🐳 2. Start Airflow with Docker

Make sure your Docker environment is up and running.

### 🌐 3. Access Airflow Web UI

Once the services are running, open the Airflow web interface in your browser:

```bash
http://localhost:8080
```

You can also verify that the webserver is running using:

```bash
curl http://localhost:8080
```

### 🔐 4. Login to Airflow

```bash
Username: admin
Password: admin
```

## 🔄 Data Flow
1. Python script generates real-time orders
2. PostgreSQL stores transactional data
3. Debezium captures changes (CDC)
4. Kafka streams events in real-time
5. Python consumer processes data
6. Data is stored in MinIO (raw layer)
7. Airflow schedules transformation jobs
8. dbt transforms data into analytics tables
9. Dashboard visualizes insights

## 📊 Use Cases
- E-commerce analytics
- Real-time dashboards
- Data warehousing pipelines
