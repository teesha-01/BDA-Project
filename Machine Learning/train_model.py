from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "supply_chain_transactions"

# Create Spark session
spark = SparkSession.builder \
    .appName("SupplyChainFraudDetection") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

def process_pipeline():
    # Define Schema for Supply Chain Data
    schema = StructType([
        StructField("Project_Code", StringType(), True),
        StructField("PQ#", StringType(), True),
        StructField("PO_SO#", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Managed_By", StringType(), True),
        StructField("Fulfill_Via", StringType(), True),
        StructField("Vendor_INCO_Term", StringType(), True),
        StructField("Shipment_Mode", StringType(), True),
        StructField("PQ_First_Sent_to_Client_Date", StringType(), True),
        StructField("PO_Sent_to_Vendor_Date", StringType(), True),
        StructField("Scheduled_Delivery_Date", StringType(), True),
        StructField("Delivered_to_Client_Date", StringType(), True),
        StructField("Delivery_Recorded_Date", StringType(), True),
        StructField("Product_Group", StringType(), True),
        StructField("Sub_Classification", StringType(), True),
        StructField("Vendor", StringType(), True),
        StructField("Item_Description", StringType(), True),
        StructField("Molecule_Brand", StringType(), True),
        StructField("Dosage", StringType(), True),
        StructField("Dosage_Form", StringType(), True),
        StructField("Unit_of_Measure_Per_Pack", StringType(), True),
        StructField("Line_Item_Quantity", IntegerType(), True),
        StructField("Line_Item_Value", DoubleType(), True),
        StructField("Pack_Price", DoubleType(), True),
        StructField("Unit_Price", DoubleType(), True),
        StructField("Manufacturing_Site", StringType(), True),
        StructField("First_Line_Designation", StringType(), True),
        StructField("Weight_Kilograms", DoubleType(), True),
        StructField("Freight_Cost_USD", DoubleType(), True),
        StructField("Line_Item_Insurance_USD", DoubleType(), True),
        StructField("Fraud_Label", IntegerType(), True)
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

# Parse Kafka messages
transactions = process_pipeline()

# Preprocess data
# Index categorical columns
categorical_columns = [
    "Country", "Managed_By", "Fulfill_Via", "Vendor_INCO_Term", "Shipment_Mode",
    "Product_Group", "Sub_Classification", "Vendor", "First_Line_Designation",
    "Molecule_Brand", "Dosage", "Dosage_Form", "Manufacturing_Site"
]
indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index") for col in categorical_columns]

# Assemble features
assembler = VectorAssembler(
    inputCols=[
        "Line_Item_Quantity", "Line_Item_Value", "Pack_Price", "Unit_Price",
        "Weight_Kilograms", "Freight_Cost_USD", "Line_Item_Insurance_USD"
    ] + [f"{col}_index" for col in categorical_columns],
    outputCol="features"
)

# Train a Logistic Regression Classifier
lr = LogisticRegression(featuresCol="features", labelCol="Fraud_Label", maxIter=10)

# Create a pipeline
pipeline = Pipeline(stages=indexers + [assembler, lr])

# Train-test split
(training_data, test_data) = transactions.randomSplit([0.8, 0.2], seed=42)

# Train the model
model = pipeline.fit(training_data)

# Evaluate the model
predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator(labelCol="Fraud_Label", metricName="areaUnderROC")
roc_auc = evaluator.evaluate(predictions)
print(f"ROC AUC: {roc_auc}")

# Save the trained model
model.write().overwrite().save("/spark_models/supply_chain_fraud_model")
print("Model saved to /spark_models/supply_chain_fraud_model")

# Stop Spark session
spark.stop()
