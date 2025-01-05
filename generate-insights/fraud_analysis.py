from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "supply_chain_data"

# Function to write DataFrame to Kafka
def write_to_kafka(df, topic):
    df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", topic) \
        .save()

# Pipeline to process Kafka stream
def process_pipeline():
    spark = SparkSession.builder \
        .appName("SupplyChainFraudAnalysis") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # Define Schema
    schema = StructType([
        StructField("cf1:Project_Code", StringType(), True),
        StructField("cf1:PQ#", StringType(), True),
        StructField("cf1:PO_SO#", StringType(), True),
        StructField("cf1:Country", StringType(), True),
        StructField("cf1:Managed_By", StringType(), True),
        StructField("cf1:Fulfill_Via", StringType(), True),
        StructField("cf1:Vendor_INCO_Term", StringType(), True),
        StructField("cf1:Shipment_Mode", StringType(), True),
        StructField("cf1:PQ_First_Sent_to_Client_Date", StringType(), True),
        StructField("cf1:PO_Sent_to_Vendor_Date", StringType(), True),
        StructField("cf1:Scheduled_Delivery_Date", StringType(), True),
        StructField("cf1:Delivered_to_Client_Date", StringType(), True),
        StructField("cf1:Delivery_Recorded_Date", StringType(), True),
        StructField("cf1:Product_Group", StringType(), True),
        StructField("cf1:Sub_Classification", StringType(), True),
        StructField("cf1:Vendor", StringType(), True),
        StructField("cf1:Item_Description", StringType(), True),
        StructField("cf1:Molecule_Brand", StringType(), True),
        StructField("cf1:Dosage", StringType(), True),
        StructField("cf1:Dosage_Form", StringType(), True),
        StructField("cf1:Unit_of_Measure_Per_Pack", StringType(), True),
        StructField("cf1:Line_Item_Quantity", DoubleType(), True),
        StructField("cf1:Line_Item_Value", DoubleType(), True),
        StructField("cf1:Pack_Price", DoubleType(), True),
        StructField("cf1:Unit_Price", DoubleType(), True),
        StructField("cf1:Manufacturing_Site", StringType(), True),
        StructField("cf1:First_Line_Designation", StringType(), True),
        StructField("cf1:Weight_Kilograms", DoubleType(), True),
        StructField("cf1:Freight_Cost_USD", DoubleType(), True),
        StructField("cf1:Line_Item_Insurance_USD", DoubleType(), True),
        StructField("cf1:Fraud_Label", StringType(), True)
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

    return parsed_df

casted_df = process_pipeline()

# Fraud Analysis by Product Group
fraud_product_group = casted_df.groupBy("cf1:Product_Group") \
    .agg(
        count(when(col("cf1:Fraud_Label") == "1", 1)).alias("fraud_count"),
        count("cf1:Project_Code").alias("total_orders"),
        (count(when(col("cf1:Fraud_Label") == "1", 1)) / count("cf1:Project_Code") * 100).alias("fraud_percentage")
    )

# Fraud Analysis by Country
fraud_country = casted_df.groupBy("cf1:Country") \
    .agg(
        count(when(col("cf1:Fraud_Label") == "1", 1)).alias("fraud_count"),
        count("cf1:Project_Code").alias("total_orders"),
        (count(when(col("cf1:Fraud_Label") == "1", 1)) / count("cf1:Project_Code") * 100).alias("fraud_percentage")
    )

# Fraud Analysis by Vendor
fraud_vendor = casted_df.groupBy("cf1:Vendor") \
    .agg(
        count(when(col("cf1:Fraud_Label") == "1", 1)).alias("fraud_count"),
        count("cf1:Project_Code").alias("total_orders"),
        (count(when(col("cf1:Fraud_Label") == "1", 1)) / count("cf1:Project_Code") * 100).alias("fraud_percentage")
    )

# Fraud Analysis by Shipment Mode
fraud_shipment_mode = casted_df.groupBy("cf1:Shipment_Mode") \
    .agg(
        count(when(col("cf1:Fraud_Label") == "1", 1)).alias("fraud_count"),
        count("cf1:Project_Code").alias("total_orders"),
        (count(when(col("cf1:Fraud_Label") == "1", 1)) / count("cf1:Project_Code") * 100).alias("fraud_percentage")
    )

# Fraud Analysis by Freight Cost Group
fraud_freight_cost = casted_df.withColumn("cost_group", 
    when(col("cf1:Freight_Cost_USD") < 500, "Low Cost")
    .when((col("cf1:Freight_Cost_USD") >= 500) & (col("cf1:Freight_Cost_USD") < 1500), "Medium Cost")
    .when(col("cf1:Freight_Cost_USD") >= 1500, "High Cost")
) \
.groupBy("cost_group") \
.agg(
    count(when(col("cf1:Fraud_Label") == "1", 1)).alias("fraud_count"),
    count("cf1:Project_Code").alias("total_orders"),
    (count(when(col("cf1:Fraud_Label") == "1", 1)) / count("cf1:Project_Code") * 100).alias("fraud_percentage")
)

# Define Kafka topics
FRAUD_PRODUCT_GROUP_TOPIC = "fraud_product_group"
FRAUD_COUNTRY_TOPIC = "fraud_country"
FRAUD_VENDOR_TOPIC = "fraud_vendor"
FRAUD_SHIPMENT_MODE_TOPIC = "fraud_shipment_mode"
FRAUD_FREIGHT_COST_TOPIC = "fraud_freight_cost"

# Write results to Kafka
write_to_kafka(fraud_product_group, FRAUD_PRODUCT_GROUP_TOPIC)
write_to_kafka(fraud_country, FRAUD_COUNTRY_TOPIC)
write_to_kafka(fraud_vendor, FRAUD_VENDOR_TOPIC)
write_to_kafka(fraud_shipment_mode, FRAUD_SHIPMENT_MODE_TOPIC)
write_to_kafka(fraud_freight_cost, FRAUD_FREIGHT_COST_TOPIC)

print("Fraud analysis results written to Kafka")

# Show results in Spark UI
fraud_product_group.show()
fraud_country.show()
fraud_vendor.show()
fraud_shipment_mode.show()
fraud_freight_cost.show()
