from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FraudAgeAnalytics") \
    .getOrCreate()

# Kafka configuration
kafka_broker = "kafka:9092"
topic = "fraud_age_analytics"

# Define the schema for JSON data
schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("user_id", StringType()) \
    .add("product_id", StringType()) \
    .add("category", StringType()) \
    .add("sub_category", StringType()) \
    .add("price", StringType()) \
    .add("quantity", StringType()) \
    .add("total_amount", StringType()) \
    .add("payment_method", StringType()) \
    .add("timestamp", StringType()) \
    .add("user_device", StringType()) \
    .add("user_city", StringType()) \
    .add("user_age", StringType()) \
    .add("user_gender", StringType()) \
    .add("user_income", StringType()) \
    .add("fraud_label", StringType())

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .load()

# Parse the JSON messages
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write parsed data to the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
