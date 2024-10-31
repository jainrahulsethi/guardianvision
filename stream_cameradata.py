# Databricks notebook source
!pip install opencv_python
dbutils.library.restartPython() 

# COMMAND ----------

# MAGIC %run ../guardianvision_v2/configs/config

# COMMAND ----------

capture_frame_path = config["api"]["capture_frame_path"]
checkpoint_loc  = config["guardianvision"]["checkpoint_loc"]

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import subprocess
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta

client_table = config["tables"]["client_master"]
# Initialize Spark session
spark = SparkSession.builder.appName("CameraStreaming").getOrCreate()

# Read data from the table
camera_df = spark.sql(f"SELECT cam_id, rtsp_url, client_id, site_id FROM {client_table} WHERE is_active = TRUE")

# Show the results
camera_df.show()


# COMMAND ----------

# Create a streaming DataFrame with Spark's `rate` source to simulate time-based events
rate_stream_df = (spark
                  .readStream
                  .format("rate")
                  .option("rowsPerSecond", 1)  # Generates one row every second
                  .load())

# Join the static camera DataFrame with the rate stream to simulate streaming
streaming_camera_df = (rate_stream_df
                       .select("timestamp")
                       .join(camera_df, how="cross"))

# Define a function to call the external Python script in parallel
def call_capture_script(partition):
    with ProcessPoolExecutor() as executor:
        futures = []
        for row in partition:
            rtsp_url = row["rtsp_url"]
            camera_id = row["cam_id"]
            client_id = row["client_id"]
            site_id = row["site_id"]
            print(f"Attempting to capture frame for camera_id: {camera_id}, rtsp_url: {rtsp_url}")
            # Run each camera capture command in a separate process
            futures.append(
                executor.submit(
                    subprocess.run,
                    ["python3", capture_frame_path, rtsp_url, str(camera_id), str(client_id), str(site_id)],
                    check=True
                )
            )
        # Wait for all futures to complete
        for future in futures:
            try:
                future.result()
            except subprocess.CalledProcessError as e:
                print(f"Error in subprocess for camera: {e}")

# Apply mapPartitions to process each partition by calling the external script
def process_batch(batch_df, batch_id):
    # Deduplicate rows by `camera_id` to ensure unique processing per camera
    unique_cameras_df = batch_df.dropDuplicates(["cam_id"])
    unique_cameras_df.rdd.foreachPartition(call_capture_script)

# Start the streaming query with a 30-second trigger interval
query = (streaming_camera_df
         .writeStream
         .outputMode("append")
         .trigger(processingTime="30 seconds")
         .foreachBatch(process_batch)
         .option("checkpointLocation", checkpoint_loc)  # Adjusted checkpoint location
         .start())
