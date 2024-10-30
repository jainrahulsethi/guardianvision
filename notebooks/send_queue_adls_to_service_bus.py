# Databricks notebook source
# DBTITLE 1,read incremental data from adls to service bus
from pyspark.sql.types import StructType, StringType, TimestampType, LongType, BinaryType
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import json
import uuid
from pyspark.sql.functions import regexp_extract

# Define schema for the binary file metadata
schema = StructType() \
    .add("path", StringType()) \
    .add("modificationTime", TimestampType()) \
    .add("length", LongType()) \
    .add("content", BinaryType())

# Azure Service Bus configuration
connection_str =dbutils.secrets.get(scope="guardian_connection_str", key="connection_str_Service_Bus")  # Replace with your connection string
queue_name = "guardianvision"  # Replace with your queue name

# Define the function to send each batch to Service Bus
def send_batch_to_service_bus(batch_df, batch_id):
    # Create a Service Bus client
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_str, logging_enable=True)
    
    # Open the client
    with servicebus_client:
        # Create a sender for the specified queue
        sender = servicebus_client.get_queue_sender(queue_name=queue_name)
        with sender:
            for row in batch_df.collect():
                # Generate a unique random ID for each message
                random_id = str(uuid.uuid4())
                
                # Construct a JSON message for each row
                message_body = {
                    "id": f"{row.stationid_camid}_{random_id}",
                    "filepath": row.path
                }
                message = ServiceBusMessage(json.dumps(message_body))
                
                # Send the message to Service Bus
                sender.send_messages(message)

# Use Auto Loader to incrementally read binary files from all subdirectories
image_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.includeExistingFiles", "true")  # Include existing files on the first run
    .option("cloudFiles.useIncrementalListing", "true")  # Enable incremental listing
    .schema(schema)
    .load("/mnt/retaildls/guardianvision/frame_data/*")  # Use wildcard to include all subdirectories
)

# Prepare the DataFrame with necessary transformations
def transform_and_send(batch_df, batch_id):
    # Extract the station ID and camera ID from the file path
    transformed_df = batch_df.withColumn("stationid_camid", regexp_extract("path", r"/(Camera_\d+_Site_\d+)/", 1)).select('path', 'stationid_camid')
    
    # Send the transformed batch to Azure Service Bus
    send_batch_to_service_bus(transformed_df, batch_id)

# Define the checkpoint path
checkpoint_path = "/mnt/checkpoint/guardianvision"

# Stream data to Azure Service Bus using foreachBatch with checkpointing
query = image_stream.writeStream \
    .foreachBatch(transform_and_send) \
    .trigger(processingTime="1 second") \
    .option("checkpointLocation", checkpoint_path) \
    .start()

#.trigger(availableNow=True) 

