
# Ecommerce Fraud Detection Pipeline

## Overview
This project demonstrates real-time fraud detection for an eCommerce platform using a big data pipeline. It involves data ingestion, storage, processing, and visualization using cutting-edge technologies.

## Key Features
- **Real-time data ingestion** with Kafka.
- **Data storage and management** in HBase.
- **Fraud detection analytics** using Apache Spark.
- **Visualization of results** through a real-time dashboard.

---

## Prerequisites
Ensure the following software is installed on your system:
- **Docker**: For containerized deployment.
- **Node.js**: For Kafka producer scripts.
- **Python 3.x**: For data processing and dashboard scripts.

---

## Getting Started

### 1. Start the Docker Cluster
To start all necessary services (Kafka, Spark, HBase, etc.), run:
```bash
docker-compose up -d
```

### 2. Kafka Setup
Create the necessary Kafka topics using the following commands:
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic pharmaceutical_supply_chain --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic fraud_analytics --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic shipment_mode_analytics --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic weight_analytics --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic vendor_analytics --partitions 3 --replication-factor 1
```

#### List All Created Topics to Verify
Run the following command to list all created Kafka topics:
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Run the Kafka producer script:
```bash
cd kafka-producer
node producer.js
```

### 3. HBase Setup

#### a. Create HBase Tables
Run the following commands to create the necessary HBase tables:
```bash
create 'pharmaceutical_supply_chain', 'transaction_data', 'analytics_data'
create 'fraud_detection', 'fraud_metrics', 'transaction_info'
```

### 4. Spark Setup

#### a. Access the Spark Master Container
Log into the Spark Master container using the following command:
```bash
docker exec -it spark-master bash
```

#### b. Install Required Dependencies
Inside the Spark Master container, run the following commands to install the necessary dependencies:
```bash
apt update
apt install -y build-essential
pip install happybase
```

#### c. Copy Processing Scripts
To copy the required Python scripts to the Spark Master container, run the following commands:
```bash
docker cp fraud_analysis.py spark-master:/tmp/fraud_analysis.py
docker cp read_kafka.py spark-master:/tmp/read_kafka.py
```

#### d. Run Spark Processing Scripts

1. Execute the fraud analysis script:
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 /tmp/fraud_analysis.py
   ```

2. Execute the Kafka Data Reading Script:
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 /tmp/read_kafka.py
   ```

### 5. Visualization Dashboard

#### a. Start the Dashboard
Navigate to the dashboard directory and start the Streamlit application:
```bash
cd dashboard
streamlit run app.py
```

#### b. Access the Dashboard
Open your browser and navigate to the following URL to access the dashboard:
```plaintext
http://localhost:8501/
```
