An End-to-End Data Engineering project that simulates an E-commerce platform, captures real-time data changes (CDC), processes them using Apache Spark, and stores them in ClickHouse for analytics. The entire workflow is containerized using Docker and orchestrated by Airflow.

## Architecture

The pipeline follows a modern **Kappa Architecture** pattern:
<img width="1054" height="403" alt="image" src="https://github.com/user-attachments/assets/3e4faa18-326d-40ec-809e-8df55ab5d541" />

Data Source (Mock E-commerce): A Python script generating random Customers, Products, Orders, and Order Items.
OLTP Database: PostgreSQL 16 (configured with wal_level=logical).
Ingestion (CDC): Debezium (running on Kafka Connect) captures row-level changes.
Message Broker: Confluent Kafka & Zookeeper.
Stream Processing: Apache Spark 3.5 (PySpark) reads from Kafka, flattens JSON payloads, handles CDC logic (insert/update/delete), and writes to ClickHouse.
OLAP Sink: ClickHouse (MergeTree engine) for high-performance analytics.
Orchestration: Apache Airflow 2.10 managing batch jobs and reporting.

## Tech Stack
Language: Python (PySpark, Faker, Airflow DAGs)
Containerization: Docker, Docker Compose
Databases: PostgreSQL 16, ClickHouse 24+
Streaming: Apache Kafka, Debezium 2.6
Processing: Apache Spark 3.5 (Structured Streaming)
Orchestration: Apache Airflow 2.10

## Installation & Running
1. Clone the repository
git clone git@github.com:HowardZeng123/realtime-cdc-pipeline-docker.git
cd realtime-cdc-pipeline-docker
2. Grant permissions (Linux/WSL only)
chmod +x script/*.sh
mkdir -p logs
chmod -R 777 logs
3. docker-compose up --build -d
4. Activate Debezium CDC (Crucial Step)
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
http://localhost:8083/connectors/ -d @register-postgres.json
