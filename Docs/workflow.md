# E-commerce Big Data Pipeline Workflow

## 1. Data Generation
- Generate synthetic e-commerce data (~1.25GB) using a data generation script.
- Oversample the dataset to ensure diversity and volume.

## 2. Data Ingestion
- *Kafka Producer*: Stream the generated dataset into Kafka topics.
- *Kafka Topics*: Create multiple topics such as:
  - pharmaceutical_supply_chain
  - fraud_analysis
  - freight_cost_analytics
  - product_group_analytics
- These topics segment data for better processing and analysis.

## 3. Data Storage
- Use Kafka consumers to fetch data from Kafka topics.
- Store the ingested data into *HBase* for hybrid database processing and querying.

## 4. Data Processing
- Use *Apache Spark* for:
  - Real-time analytics.
  - Batch processing.
- Key tasks performed:
  - Fraud detection.
  - Cost optimization.
  - Product category and sales analytics.
- Processed insights are sent back into Kafka topics for further use or analysis.

## 5. Data Analysis
- Perform *Exploratory Data Analysis (EDA)* using Spark:
  - Fraud trends.
  - Sales distribution.
  - Shipment costs and optimizations.
- Insights are prepared for visualization and decision-making.

## 6. Storage of Insights
- Store the processed and analyzed insights in *HBase*.
- Insights are structured for fast querying and dashboard integration.

## 7. Visualization
- Create interactive dashboards using *Streamlit*.
- Dashboards include:
  - Fraud detection rates.
  - Cost optimization metrics.
  - Product and sales trends.
- Enable both admin and user-level views for actionable insights.

## 8. Orchestration and Automation
- Use *Apache Airflow* for automating tasks:
  - Data ingestion.
  - Processing.
  - Visualization updates.
- Scheduled workflows ensure seamless execution of all components.




### Summary
This workflow ensures an efficient and scalable pipeline for processing large-scale e-commerce data. By leveraging modern big data technologies, the project delivers real-time analytics, enabling businesses to make data-driven decisions effectively.