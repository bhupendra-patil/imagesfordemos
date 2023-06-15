# Databricks notebook source


# COMMAND ----------

# Import the required libraries
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient

# Set the credential object
credential = DefaultAzureCredential()

# Set the file system and file path
file_system_name = "<file_system_name>"
file_path = "<file_path>"

# Create a DataLakeFileClient object
file_client = DataLakeFileClient.from_connection_string("<connection-string>", file_system_name=file_system_name, file_path=file_path, credential=credential)

# Read the contents of the file
file_contents = file_client.read_file()

# Print the contents of the file
print(file_contents)

# COMMAND ----------

# MAGIC %md
# MAGIC ## jst adding commetns

# COMMAND ----------

# Import the required libraries
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timezone

# Define the schema of the Kafka JSON messages
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("field1", DoubleType(), True),
    StructField("field2", DoubleType(), True),
    StructField("field3", DoubleType(), True)
])

# Define the options
kafka_bootstrap_servers = "<kafka_bootstrap_servers>"
kafka_topic = "<kafka_topic>"
checkpoint_location = "/mnt/<delta_location>/"

# Define the Delta table location
delta_table_path = "/mnt/<delta_location>/"

# Read from Kafka stream and convert to DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("json")) \
    .select("json.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").cast(TimestampType()))

# Write the stream to Delta table
query = df \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .table("db_name.table_name")

# Start the streaming query
query.start()

# Wait for the query to terminate
query.awaitTermination()

# COMMAND ----------

# Import the required libraries
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timezone

# Define the schema of the Kafka JSON messages
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("field1", DoubleType(), True),
    StructField("field2", DoubleType(), True),
    StructField("field3", DoubleType(), True)
])

# Define the options
kafka_bootstrap_servers = "<kafka_bootstrap_servers>"
kafka_topic = "<kafka_topic>"
checkpoint_location = "/mnt/<delta_location>/"

# Define the Delta table location
delta_table_path = "/mnt/<delta_location>/"

# Read from Kafka stream and convert to DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("json")) \
    .select("json.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").cast(TimestampType()))

# Write the stream to Delta table
query = df \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .table("db_name.table_name")

# Start the streaming query
query.start()

# Wait for the query to terminate
query.awaitTermination()

# COMMAND ----------


