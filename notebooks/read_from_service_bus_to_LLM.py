# Databricks notebook source
# DBTITLE 1,utility containing LLM
# MAGIC %run ../notebooks/Utility

# COMMAND ----------

# DBTITLE 1,read messages to LLM
from azure.servicebus import ServiceBusClient
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import ast
import re


rag_search = GuardianVisionRAG()

query_text = "Safety checklist for construction activity with scafolding and heavy machinery" 
#with heavy machinery operation. Make sure not to miss on the applicable high priority items"
result = rag_search.perform_search(query_text)

prompt = "Based on the provided checklist, assign a safety score from 1 to 5. Assess only what is visible in the image, and avoid penalizing for items that may not be captured due to camera limitations. For unsafe cases, the rating should be only 1 or 2 and for safe 4 or 5. Your response should be a JSON object with the following keys: safety_rating, safety_violation_category (optional, may be null if no violation is detected), and one_sentence_description. Here is the checklist: " + result
# Azure Service Bus configuration
connection_str = dbutils.secrets.get(scope="guardian_connection_str", key="connection_str_Service_Bus")
queue_name = "guardianvision"

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Schema for DataFrame
schema = StructType([
    StructField("image_path", StringType(), True),
    StructField("client_id", StringType(), True), 
    StructField("site_id", StringType(), True),  # Update for site_id
    StructField("cam_id", StringType(), True),   
    StructField("safety_rating", StringType(), True),
    StructField("safety_violation_category", StringType(), True),
    StructField("one_sentence_description", StringType(), True)
])

# List to collect rows for DataFrame
data_rows = []

# Function to receive and process messages from the Azure Service Bus queue
def receive_and_process_messages(analyzer):
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_str, logging_enable=True)

    with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name=queue_name)

        with receiver:
            while True:
                messages = receiver.receive_messages(max_message_count=10, max_wait_time=1)
                if not messages:
                    break  # Exit the loop if no more messages are available

                for msg in messages:
                    message_body = json.loads(str(msg))
                    image_path = message_body.get("filepath")

                    if image_path:
 
                        # Ensure the image path exists in the message and modify to filesystem format
                        image_path = image_path.replace("dbfs:", "/dbfs", 1)
                        print(f"Processing image at: {image_path}")

                        # Extract site_id and cam_id from the image path
                        match = re.search(r"frame_(\d+)_(\d+)_", image_path)
                        if match:
                            site_id = int(match.group(1))  # Extract site_id
                            cam_id = int(match.group(2))    # Extract cam_id
                        else:
                            site_id = None 
                            cam_id = None 
                            
                        client_id = image_path.split("/")[-2]

                        # Analyze image and retrieve result JSON
                        result_str = analyzer.analyze_image(image_path, prompt)
                        print(type(result_str))
                        print(result_str)

                        # Append data to rows list
                        data_rows.append({
                            "image_path": image_path,
                            "client_id": client_id,
                            "site_id": site_id,  # Include site_id
                            "cam_id": cam_id,
                            "safety_rating": result_str["safety_rating"],
                            "safety_violation_category": result_str["safety_violation_category"],
                            "one_sentence_description": result_str["one_sentence_description"]
                        })

                        # Complete the message
                        receiver.complete_message(msg)

    print("Queue is empty.")

# Create an instance of GuardianVisionAnalyzer
analyzer_instance = GuardianVisionAnalyzer(api_base_url="https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints")

# Start processing messages
receive_and_process_messages(analyzer_instance)

# Convert collected data to DataFrame and show
if data_rows:
    df = spark.createDataFrame(data_rows, schema=schema)
else:
    print("No data was collected.")

# COMMAND ----------

# DBTITLE 1,Transformations
from datetime import datetime
from pyspark.sql.functions import current_timestamp, col
safety_df = df.withColumn("Last_updated_On", current_timestamp()).withColumn("cam_id", col("cam_id").cast("int")).withColumn("safety_rating", col("safety_rating").cast("int")).withColumnRenamed('one_sentence_description','description').withColumn("site_id", col("site_id").cast("int")).withColumn("safety_rating", col("safety_rating").cast("int"))

# COMMAND ----------

# DBTITLE 1,append to delta table
safety_df.write.mode("append").format("delta").saveAsTable("guardianvision.safety_assessment_log")
