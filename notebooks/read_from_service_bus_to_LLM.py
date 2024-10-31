# Databricks notebook source
# DBTITLE 1,utility containing LLM
# MAGIC %run ../notebooks/Utility

# COMMAND ----------

# MAGIC %run  ../configs/config

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from azure.servicebus import ServiceBusClient

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import re
import time
from datetime import datetime
from pyspark.sql.functions import current_timestamp, col

api_base_url = config["api"]["endpoint"]
client_master = config["tables"]["client_master"]
safety_assessment = config["tables"]["safety_assessment"]

rag_search = GuardianVisionRAG()
data_rows = []
max_rows_before_save = 10


# Read data from the table
client_df = spark.sql(f"SELECT cam_id, client_id, site_id, prompt_to_use, activity_description FROM {client_master} WHERE is_active = TRUE").collect()

# Convert client_df to a dictionary for easier access
client_data_dict = {(row.site_id, row.cam_id, row.client_id): (row.prompt_to_use, row.activity_description) for row in client_df}

analyzer_instance = GuardianVisionAnalyzer(api_base_url=config["api"]["endpoint"])

# Azure Service Bus configuration
connection_str = dbutils.secrets.get(scope="guardian_connection_str", key="connection_str_Service_Bus")
queue_name = "guardianvision"

def save_data_to_dataframe():
    global data_rows
    if data_rows:
        schema = StructType([
            StructField("image_path", StringType(), True),
            StructField("client_id", StringType(), True),
            StructField("site_id", StringType(), True),
            StructField("cam_id", StringType(), True),
            StructField("safety_rating", StringType(), True),
            StructField("safety_violation_category", StringType(), True),
            StructField("one_sentence_description", StringType(), True)])
        df = spark.createDataFrame(data_rows,schema).withColumn("Last_updated_On", current_timestamp()).withColumn("cam_id", col("cam_id").cast("int")).withColumn("safety_rating", col("safety_rating").cast("int")).withColumnRenamed('one_sentence_description','description').withColumn("site_id", col("site_id").cast("int")).withColumn("safety_rating", col("safety_rating").cast("int"))
        df.write.mode("append").format("delta").saveAsTable(safety_assessment)
        data_rows = []

def process_with_llm(message_body):
    global client_data_dict
    # Simulate LLM processing (replace with actual LLM call)
    path = json.loads(str(message_body))
    image_path = path.get("filepath") 
    image_id = path.get("id")   
    print(f"Processing with LLM: {image_path} {datetime.now()}")

    if image_path:
        image_path = image_path.replace("dbfs:", "/dbfs", 1)

        # Extract site_id and cam_id from the image path
        match = re.search(r"frame_(\d+)_(\d+)_", image_path)
        if match:
            site_id = int(match.group(1))
            cam_id = int(match.group(2))
        else:
            site_id = None 
            cam_id = None 

        # Extract client_id from the image path
        client_id = image_path.split("/")[-2]

        # Check if site_id, cam_id, and client_id exist in client_data_dict
        prompt_to_use, activity_description = client_data_dict.get((int(site_id), int(cam_id), int(client_id)), (None, None))

        if prompt_to_use:
            # Use the available prompt directly
            prompt = "Based on the provided checklist, assign a safety score from 1 to 5. Assess only what is visible in the image, and avoid penalizing for items that may not be captured due to camera limitations. For unsafe cases, the rating should be only 1 or 2 and for safe 4 or 5. Your response should be a JSON object with the following keys: safety_rating, safety_violation_category (optional, may be null if no violation is detected), and one_sentence_description. Here is the checklist: " + prompt_to_use
        elif activity_description:
            # Use the activity description if available
            query_text = activity_description
            result = rag_search.perform_search(query_text)
            prompt = "Based on the provided checklist, assign a safety score from 1 to 5. Assess only what is visible in the image, and avoid penalizing for items that may not be captured due to camera limitations. For unsafe cases, the rating should be only 1 or 2 and for safe 4 or 5. Your response should be a JSON object with the following keys: safety_rating, safety_violation_category (optional, may be null if no violation is detected), and one_sentence_description. Here is the checklist: " + result
            spark.sql(f"""
                UPDATE {client_master}
                SET prompt_to_use = '{result}' 
                WHERE site_id = {site_id} AND cam_id = {cam_id} AND client_id = {client_id}
                """)
        else:
            # Use hardcoded query_text if neither is available
            query_text = "Safety checklist for construction activity with scaffolding and heavy machinery"
            result = rag_search.perform_search(query_text)
            prompt = "Based on the provided checklist, assign a safety score from 1 to 5. Assess only what is visible in the image, and avoid penalizing for items that may not be captured due to camera limitations. For unsafe cases, the rating should be only 1 or 2 and for safe 4 or 5. Your response should be a JSON object with the following keys: safety_rating, safety_violation_category (optional, may be null if no violation is detected), and one_sentence_description. Here is the checklist: " + result

        # Analyze image and retrieve result JSON
        result_str = analyzer_instance.analyze_image(image_path, prompt)
        data_rows.append({
            "image_path": image_path,
            "client_id": client_id,
            "site_id": site_id,
            "cam_id": cam_id,
            "safety_rating": result_str.get("safety_rating"),
            "safety_violation_category": result_str.get("safety_violation_category"),
            "one_sentence_description": result_str.get("one_sentence_description")
        })

        # Periodically save to DataFrame
        if len(data_rows) >= max_rows_before_save:
            save_data_to_dataframe()
    # Simulate processing time


# Function to process messages
def receive_and_process_messages():
    service_bus_client = ServiceBusClient.from_connection_string(connection_str)
    receiver = service_bus_client.get_queue_receiver(queue_name=queue_name)

    with receiver:
        while True:
            messages = receiver.receive_messages(max_message_count=10, max_wait_time=2)
            print(f"Read msg from queue for processing: {messages}")
            if messages:
                with ThreadPoolExecutor() as executor:
                    futures = {executor.submit(process_with_llm, msg): msg for msg in messages}
                    for future in futures:
                        try:
                            future.result()  # Wait for the processing to complete
                            receiver.complete_message(futures[future])  # Complete the message
                        except Exception as e:
                            print(f"Error processing message: {e}")
            else:
                save_data_to_dataframe()

# Run the receiver function
receive_and_process_messages()

