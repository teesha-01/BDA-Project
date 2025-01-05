from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "fraud_age_analytics"  # Change to your topic

def write_to_kafka(df, topic):
    df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", topic) \
        .save()

def process_pipeline():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("FraudAnalytics") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # Define schema (adjusted to your dataset)
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_device", StringType(), True),
        StructField("user_city", StringType(), True),
        StructField("user_age", StringType(), True),
        StructField("user_gender", StringType(), True),
        StructField("user_income", StringType(), True),
        StructField("fraud_label", StringType(), True),
    ])

    # Read data from Kafka
    raw_stream = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "10000") \
        .load()

    print("Reading from Kafka")

    # Deserialize JSON messages
    parsed_df = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), schema).alias("data"))
        .select("data.*")
    )

    # Cast fields to appropriate data types
    casted_df = (
        parsed_df
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))
        .withColumn("user_age", col("user_age").cast(IntegerType()))
        .withColumn("user_income", col("user_income").cast(IntegerType()))
        .withColumn("fraud_label", col("fraud_label").cast(IntegerType()))
    )

    return casted_df

# Process data
casted_df = process_pipeline()

# 1. Fraud Transaction Percentage by Category
fraud_category = casted_df.groupBy("category") \
    .agg(
        count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
        count("transaction_id").alias("total_transactions"),
        (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
    )

fraud_category.show()
print('Fraud by Category done')

# 2. Fraud Transaction Percentage by Payment Method
fraud_payment = casted_df.groupBy("payment_method") \
    .agg(
        count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
        count("transaction_id").alias("total_transactions"),
        (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
    )

fraud_payment.show()
print('Fraud by Payment Method done')

# 3. Fraud Transaction Percentage by Device Type
fraud_device = casted_df.groupBy("user_device") \
    .agg(
        count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
        count("transaction_id").alias("total_transactions"),
        (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
    )

fraud_device.show()
print('Fraud by Device done')

# 4. Fraud Transaction Percentage by City
fraud_city = casted_df.groupBy("user_city") \
    .agg(
        count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
        count("transaction_id").alias("total_transactions"),
        (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
    )

fraud_city.show()
print('Fraud by City done')

# 5. Fraud Transaction Percentage by Age Group
fraud_age = casted_df.withColumn("age_group", 
    when(col("user_age") < 20, "Under 20")
    .when((col("user_age") >= 20) & (col("user_age") < 30), "20-29")
    .when((col("user_age") >= 30) & (col("user_age") < 40), "30-39")
    .when((col("user_age") >= 40) & (col("user_age") < 50), "40-49")
    .when(col("user_age") >= 50, "50+")
) \
.groupBy("age_group") \
.agg(
    count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
    count("transaction_id").alias("total_transactions"),
    (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
)

fraud_age.show()
print('Fraud by Age Group done')

# 6. Fraud Transaction Percentage by Income Group
fraud_income = casted_df.withColumn("income_group", 
    when(col("user_income") < 20000, "Under 20k")
    .when((col("user_income") >= 20000) & (col("user_income") < 60000), "20k-59k")
    .when((col("user_income") >= 60000) & (col("user_income") < 100000), "60k-99k")
    .when((col("user_income") >= 100000) & (col("user_income") < 140000), "100k-139k")
    .when(col("user_income") >= 140000, "140k+")
) \
.groupBy("income_group") \
.agg(
    count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
    count("transaction_id").alias("total_transactions"),
    (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
)

fraud_income.show()
print('Fraud by Income done')

# Write processed data to Kafka topics
FRAUD_CATEGORY_TOPIC = "fraud_category"
FRAUD_PAYMENT_TOPIC = "fraud_payment"
FRAUD_DEVICE_TOPIC = "fraud_device"
FRAUD_CITY_TOPIC = "fraud_city"
FRAUD_AGE_TOPIC = "fraud_age"
FRAUD_INCOME_TOPIC = "fraud_income"

write_to_kafka(fraud_category, FRAUD_CATEGORY_TOPIC)
write_to_kafka(fraud_payment, FRAUD_PAYMENT_TOPIC)
write_to_kafka(fraud_device, FRAUD_DEVICE_TOPIC)
write_to_kafka(fraud_city, FRAUD_CITY_TOPIC)
write_to_kafka(fraud_age, FRAUD_AGE_TOPIC)
write_to_kafka(fraud_income, FRAUD_INCOME_TOPIC)

print("Everything written to Kafka")
