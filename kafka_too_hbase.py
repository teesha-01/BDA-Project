import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

def main():
    # Initialize Spark session with Kafka dependencies
    spark = SparkSession.builder \
        .appName("KafkaToHBase_Batch") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .getOrCreate()


    # 1) Kafka configuration (Update the Kafka server IP and port)
    kafka_bootstrap_servers = "172.22.0.13:29092"  # Kafka IP and port
    kafka_topic = "fraud_age_analytics"  # Kafka topic name (update this if needed)

    # 2) Define your schema (this remains the same)
    json_schema = StructType([
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

    # 3) Batch read from Kafka (Read the data from Kafka topic)
    df_kafka = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # 4) Parse JSON
    parsed_df = (
        df_kafka
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), json_schema).alias("data"))
        .select("data.*")
    )

    # 5) Cast numeric fields
    casted_df = (
        parsed_df
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))
        .withColumn("user_age", col("user_age").cast(IntegerType()))
        .withColumn("user_income", col("user_income").cast(IntegerType()))
        .withColumn("fraud_label", col("fraud_label").cast(IntegerType()))
    )

    # 6) Optional: Check how many rows
    total_rows = casted_df.count()
    print(f"Total rows read from Kafka: {total_rows}")

    # 7) Write to HBase (Using foreachPartition to write in batches)
    def write_partition(iter_rows):
        # Open HBase connection once per partition (Update the HBase host)
        connection = happybase.Connection(host='hbase-master', port=9090)  # Update this with the correct IP if needed
        table = connection.table('fraud_age_analytics_hbase_table')  # HBase table name

        def safe_encode(value):
            return str(value).encode() if value is not None else b''

        # Insert each row into HBase table
        for row in iter_rows:
            table.put(
                safe_encode(row.transaction_id),
                {
                    b'transaction_data:user_id': safe_encode(row.user_id),
                    b'transaction_data:product_id': safe_encode(row.product_id),
                    b'transaction_data:category': safe_encode(row.category),
                    b'transaction_data:sub_category': safe_encode(row.sub_category),
                    b'transaction_data:price': safe_encode(row.price),
                    b'transaction_data:quantity': safe_encode(row.quantity),
                    b'transaction_data:total_amount': safe_encode(row.total_amount),
                    b'transaction_data:payment_method': safe_encode(row.payment_method),
                    b'transaction_data:timestamp': safe_encode(row.timestamp),
                    b'transaction_data:user_device': safe_encode(row.user_device),
                    b'transaction_data:user_city': safe_encode(row.user_city),
                    b'transaction_data:user_age': safe_encode(row.user_age),
                    b'transaction_data:user_gender': safe_encode(row.user_gender),
                    b'transaction_data:user_income': safe_encode(row.user_income),
                    b'transaction_data:fraud_label': safe_encode(row.fraud_label),
                }
            )
        connection.close()

    # 8) Write each partition to HBase
    casted_df.foreachPartition(write_partition)

    spark.stop()

if __name__ == "__main__":
    main()
