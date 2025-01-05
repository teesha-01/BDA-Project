import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def main():
    spark = SparkSession.builder \
        .appName("KafkaToHBase_Batch") \
        .getOrCreate()

    kafka_bootstrap_servers = "kafka:9092"
    kafka_topic = "supply_chain_data"

    # Define the schema
    json_schema = StructType([
        StructField('cf1:Project_Code', StringType(), True),
    StructField('cf1:PQ#', StringType(), True),
    StructField('cf1:PO_SO#', StringType(), True),
    StructField('cf1:Country', StringType(), True),
    StructField('cf1:Managed By', StringType(), True),
    StructField('cf1:Fulfill Via', StringType(), True),
    StructField('cf1:Vendor INCO Term', StringType(), True),
    StructField('cf1:Shipment Mode', StringType(), True),
    StructField('cf1:PQ First Sent to Client Date', StringType(), True),
    StructField('cf1:PO Sent to Vendor Date', StringType(), True),
    StructField('cf1:Scheduled Delivery Date', StringType(), True),
    StructField('cf1:Delivered to Client Date', StringType(), True),
    StructField('cf1:Delivery Recorded Date', StringType(), True),
    StructField('cf1:Product Group', StringType(), True),
    StructField('cf1:Sub Classification', StringType(), True),
    StructField('cf1:Vendor', StringType(), True),
    StructField('cf1:Item Description', StringType(), True),
    StructField('cf1:Molecule_Brand', StringType(), True),
    StructField('cf1:Dosage', StringType(), True),
    StructField('cf1:Dosage Form', StringType(), True),
    StructField('cf1:Unit_of_Measure_Per_Pack', StringType(), True),
    StructField('cf1:Line Item Quantity', StringType(), True),
    StructField('cf1:Line Item Value', StringType(), True),
    StructField('cf1:Pack Price', StringType(), True),
    StructField('cf1:Unit Price', StringType(), True),
    StructField('cf1:Manufacturing Site', StringType(), True),
    StructField('cf1:First Line Designation', StringType(), True),
    StructField('cf1:Weight_Kilograms', DoubleType(), True),
    StructField('cf1:Freight Cost (USD)', DoubleType(), True),
    StructField('cf1:Line Item Insurance (USD)', DoubleType(), True)
])

    # Batch read from Kafka
    df_kafka = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # Parse JSON
    parsed_df = (
        df_kafka
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), json_schema).alias("data"))
        .select("data.*")
    )

    # Cast numeric fields
    casted_df = (
        parsed_df
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))
        .withColumn("user_age", col("user_age").cast(IntegerType()))
        .withColumn("user_income", col("user_income").cast(IntegerType()))
        .withColumn("fraud_label", col("fraud_label").cast(IntegerType()))
    )

    # Log the number of rows
    total_rows = casted_df.count()
    print(f"Total rows read from Kafka: {total_rows}")

    # Write to HBase
    def write_partition(iter_rows):
        try:
            connection = happybase.Connection(host='hbase-master', port=9091)
            table = connection.table('transaction_detail_HBase_tbl')

            def safe_encode(value):
                return str(value).encode() if value is not None else b''

            for row in iter_rows:
                table.put(
                    safe_encode(row.transaction_id),
                    {
                       StructField('cf1:Project_Code', StringType(), True),
    StructField('cf1:PQ#', StringType(), True),
    StructField('cf1:PO_SO#', StringType(), True),
    StructField('cf1:Country', StringType(), True),
    StructField('cf1:Managed By', StringType(), True),
    StructField('cf1:Fulfill Via', StringType(), True),
    StructField('cf1:Vendor INCO Term', StringType(), True),
    StructField('cf1:Shipment Mode', StringType(), True),
    StructField('cf1:PQ First Sent to Client Date', StringType(), True),
    StructField('cf1:PO Sent to Vendor Date', StringType(), True),
    StructField('cf1:Scheduled Delivery Date', StringType(), True),
    StructField('cf1:Delivered to Client Date', StringType(), True),
    StructField('cf1:Delivery Recorded Date', StringType(), True),
    StructField('cf1:Product Group', StringType(), True),
    StructField('cf1:Sub Classification', StringType(), True),
    StructField('cf1:Vendor', StringType(), True),
    StructField('cf1:Item Description', StringType(), True),
    StructField('cf1:Molecule_Brand', StringType(), True),
    StructField('cf1:Dosage', StringType(), True),
    StructField('cf1:Dosage Form', StringType(), True),
    StructField('cf1:Unit_of_Measure_Per_Pack', StringType(), True),
    StructField('cf1:Line Item Quantity', StringType(), True),
    StructField('cf1:Line Item Value', StringType(), True),
    StructField('cf1:Pack Price', StringType(), True),
    StructField('cf1:Unit Price', StringType(), True),
    StructField('cf1:Manufacturing Site', StringType(), True),
    StructField('cf1:First Line Designation', StringType(), True),
    StructField('cf1:Weight_Kilograms', DoubleType(), True),
    StructField('cf1:Freight Cost (USD)', DoubleType(), True),
    StructField('cf1:Line Item Insurance (USD)', DoubleType(), True)

                    }
                )
        except Exception as e:
            # Safely decode exception message
            error_message = str(e) if isinstance(e, str) else repr(e)
            print(f"Error writing to HBase: {error_message}")
        finally:
            if 'connection' in locals():
                connection.close()

    # Write partitions to HBase
    casted_df.foreachPartition(write_partition)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
