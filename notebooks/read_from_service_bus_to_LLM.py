# Databricks notebook source
# DBTITLE 1,utility containing LLM
# MAGIC %run ../notebooks/Utility

# COMMAND ----------

# DBTITLE 1,read messages to llm
from azure.servicebus import ServiceBusClient
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row
import ast


# Azure Service Bus configuration
connection_str = dbutils.secrets.get(scope="guardian_connection_str", key="connection_str_Service_Bus")  # Replace with your connection string
queue_name = "guardianvision"  # Replace with your queue name

# Function to receive and process messages from the Azure Service Bus queue
def receive_and_process_messages(analyzer):
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_str, logging_enable=True)

    with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name=queue_name)

        with receiver:
            while True:
                messages = receiver.receive_messages(max_message_count=10, max_wait_time=5)
                if not messages:
                    break  # Exit the loop if no more messages are available

                for msg in messages:
                    message_body = json.loads(str(msg))
                    image_path = message_body.get("filepath")

                    if image_path:
                        try:
                            # Ensure the image path exists in the message
                            image_path = image_path.replace("dbfs:", "/dbfs", 1)
                            print(image_path)
                            result_str = analyzer.analyze_image(image_path, prompt)
                            print("Analysis Result:\n", result_str)

                            # Complete the message
                            receiver.complete_message(msg)
                        except Exception as e:
                            print(e)
                            pass

    print("Queue is empty.")

# Create an instance of GuardianVisionAnalyzer
analyzer_instance = GuardianVisionAnalyzer(api_base_url="https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints")
# Start processing messages
receive_and_process_messages(analyzer_instance)


# COMMAND ----------

# 1. code separate 
# 2. consider - table push -- Done 
# 3. dataframe to adls - image push -- done 
# 4. Kissflow call logging 

# COMMAND ----------

# DBTITLE 1,LLM Json Output to Delta Table
from pyspark.sql import SparkSession
import random
from datetime import datetime
from pyspark.sql.functions import current_timestamp,col



# Simulated variables
site_id = "site_01"  # Example site ID
current_date = datetime.now().strftime("%Y-%m-%d")  # Get current date

# Simulated loop where JSON data and image_path are generated
data = []

for cam_id in range(1, 6):  # Simulating camera IDs from 1 to 5
    # Example JSON data (this would come from your actual logic)
    json_data = {
        "safety_rating": f"{cam_id}",
        "safety_category": ["Critical", "High", "Moderate", "Low", "Minimal"][cam_id - 1],
        "description": [
            "Requires immediate attention and remediation.",
            "Significant risks that need to be addressed promptly.",
            "Moderate risks that may require monitoring.",
            "Low risks that are manageable with regular oversight.",
            "Minimal risks that do not require immediate action."
        ][cam_id - 1]
    }
    
    # Constructing the image path with the format: site_id_camera_id_current_date.jpg
    image_path = f"{site_id}_{cam_id}_{current_date}.jpg"
    
    # Append the data as a tuple, including camera_id
    data.append((json_data['safety_rating'], json_data['safety_category'], json_data['description'], image_path, cam_id))

# Create a DataFrame from the collected data
safety_df = spark.createDataFrame(data, schema=["safety_rating", "safety_category", "description", "image_path", "cam_id"]).withColumn("Last_updated_On", current_timestamp()).withColumn("cam_id", col("cam_id").cast("int")).withColumn("safety_rating", col("safety_rating").cast("int"))




# COMMAND ----------

# DBTITLE 1,append to delta table
safety_df.write.mode("append").format("delta").saveAsTable("guardianvision.safety_assessment_log")

# COMMAND ----------

# %sql
# CREATE or replace table  guardianvision.safety_assessment_log(
#     image_path string COMMENT 'path of the image file',
#     cam_id int comment 'id of the camera from which the image is captured',
#     safety_rating INT COMMENT 'safety rating from 1-5',
#     safety_category string,
#     description string COMMENT 'one liner comment explaining the reasoning behind the safety rating',
#     last_updated_on timestamp comment 'timestamp of the record being inserted'
# )
# COMMENT 'This table stores the safety assessment data for images captured by the camera, including safety ratings and comments on observed conditions.';


# COMMAND ----------

# %sql
# select * from  guardianvision.safety_assessment_log
