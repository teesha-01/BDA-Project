from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, sum, max, min, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Spark Session
spark = SparkSession.builder \
    .appName("SupplyChainAnalytics") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "supply_chain_data"
CATEGORY_TOPIC = "category_analytics"
VENDOR_TOPIC = "vendor_analytics"
COUNTRY_TOPIC = "country_analytics"
SHIPMENT_MODE_TOPIC = "shipment_mode_analytics"
FREIGHT_COST_TOPIC = "freight_cost_analytics"

# Define Schema
schema = StructType([
    StructField("cf1:Project_Code", StringType(), True),
    StructField("cf1:PQ#", StringType(), True),
    StructField("cf1:PO_SO#", StringType(), True),
    StructField("cf1:Country", StringType(), True),
    StructField("cf1:Managed By", StringType(), True),
    StructField("cf1:Fulfill Via", StringType(), True),
    StructField("cf1:Vendor INCO Term", StringType(), True),
    StructField("cf1:Shipment Mode", StringType(), True),
    StructField("cf1:PQ First Sent to Client Date", StringType(), True),
    StructField("cf1:PO Sent to Vendor Date", StringType(), True),
    StructField("cf1:Scheduled Delivery Date", StringType(), True),
    StructField("cf1:Delivered to Client Date", StringType(), True),
    StructField("cf1:Delivery Recorded Date", StringType(), True),
    StructField("cf1:Product Group", StringType(), True),
    StructField("cf1:Sub Classification", StringType(), True),
    StructField("cf1:Vendor", StringType(), True),
    StructField("cf1:Item Description", StringType(), True),
    StructField("cf1:Molecule_Brand", StringType(), True),
    StructField("cf1:Dosage", StringType(), True),
    StructField("cf1:Dosage Form", StringType(), True),
    StructField("cf1:Unit_of_Measure_Per_Pack", StringType(), True),
    StructField("cf1:Line Item Quantity", StringType(), True),
    StructField("cf1:Line Item Value", DoubleType(), True),
    StructField("cf1:Pack Price", DoubleType(), True),
    StructField("cf1:Unit Price", DoubleType(), True),
    StructField("cf1:Manufacturing Site", StringType(), True),
    StructField("cf1:First Line Designation", StringType(), True),
    StructField("cf1:Weight_Kilograms", DoubleType(), True),
    StructField("cf1:Freight Cost (USD)", DoubleType(), True),
    StructField("cf1:Line Item Insurance (USD)", DoubleType(), True)
])

# Read data from Kafka
raw_stream = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON messages
parsed_df = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), schema).alias("data"))
    .select("data.*")
)

# Analytics
print("Processing data...")

# 1. Product Group Analytics
category_analytics = parsed_df.groupBy("cf1:Product Group") \
    .agg(
        avg("cf1:Line Item Value").alias("avg_line_item_value"),
        count("cf1:PQ#").alias("total_orders"),
        max("cf1:Line Item Value").alias("max_line_item_value"),
        min("cf1:Line Item Value").alias("min_line_item_value")
    )

# 2. Vendor Analytics
vendor_analytics = parsed_df.groupBy("cf1:Vendor") \
    .agg(
        count("cf1:PQ#").alias("total_orders"),
        avg("cf1:Freight Cost (USD)").alias("avg_freight_cost")
    )

# 3. Country Analytics
country_analytics = parsed_df.groupBy("cf1:Country") \
    .agg(
        count("cf1:PQ#").alias("total_orders"),
        sum("cf1:Freight Cost (USD)").alias("total_freight_cost"),
        avg("cf1:Line Item Value").alias("avg_line_item_value")
    )

# 4. Shipment Mode Analytics
shipment_mode_analytics = parsed_df.groupBy("cf1:Shipment Mode") \
    .agg(
        count("cf1:PQ#").alias("total_orders"),
        avg("cf1:Weight_Kilograms").alias("avg_weight"),
        sum("cf1:Freight Cost (USD)").alias("total_freight_cost")
    )

# 5. Freight Cost Distribution
freight_cost_analytics = parsed_df.select("cf1:Freight Cost (USD)") \
    .agg(
        avg("cf1:Freight Cost (USD)").alias("avg_freight_cost"),
        max("cf1:Freight Cost (USD)").alias("max_freight_cost"),
        min("cf1:Freight Cost (USD)").alias("min_freight_cost")
    )

# Write to Kafka
def write_to_kafka(df, topic):
    df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", topic) \
        .save()

# Writing analytics results to Kafka
write_to_kafka(category_analytics, CATEGORY_TOPIC)
write_to_kafka(vendor_analytics, VENDOR_TOPIC)
write_to_kafka(country_analytics, COUNTRY_TOPIC)
write_to_kafka(shipment_mode_analytics, SHIPMENT_MODE_TOPIC)
write_to_kafka(freight_cost_analytics, FREIGHT_COST_TOPIC)

print("Analytics results written to Kafka")

# Display results for debugging
category_analytics.show()
vendor_analytics.show()
country_analytics.show()
shipment_mode_analytics.show()
freight_cost_analytics.show()

# Await termination for streams
spark.streams.awaitAnyTermination()
