# ecommerce-data-engineering-pipeline
🚀 Real-time E-commerce Data Engineering Pipeline

📌 Overview
This small project demonstrates a real-time data pipeline using some modern and popular Data Engineering tools.

The system simulates an e-commerce environment where users continuously place orders, and the data flow through a complete pipeline from ingestion to analytics and visualization.

🏗️ Architechture
PostgreSQL → Debezium → Kafka → Python Consumer → MinIO (Data Lake) → Airflow → dbt → Data Warehouse → Dashboard

⚙️ Technologies
- Python – Data generation & processing
- PostgreSQL – Source database
- Debezium – Change Data Capture (CDC)
- Apache Kafka – Real-time streaming
- MinIO – Data lake storage
- Apache Airflow – Workflow orchestration
- dbt – Data transformation

🔄 Data Flow
1. Python script generates real-time orders
2. PostgreSQL stores transactional data
3. Debezium captures changes (CDC)
4. Kafka streams events in real-time
5. Python consumer processes data
6. Data is stored in MinIO (raw layer)
7. Airflow schedules transformation jobs
8. dbt transforms data into analytics tables
9. Dashboard visualizes insights

📊 Use Cases
- E-commerce analytics
- Real-time dashboards
- Data warehousing pipelines
