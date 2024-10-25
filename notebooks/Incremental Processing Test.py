# Databricks notebook source
from pyspark.sql.types import StructType, StringType, TimestampType, LongType, BinaryType
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("IncrementalFileIngestion").getOrCreate()

# Define schema for the binary file metadata
schema = StructType() \
    .add("path", StringType()) \
    .add("modificationTime", TimestampType()) \
    .add("length", LongType()) \
    .add("content", BinaryType())

# Define Delta table path and checkpoint path
delta_table_path = "main.default.test_images_path_3"
checkpoint_path = "/mnt/checkpoint/guardianvision"

# Use Auto Loader to incrementally read binary files from all subdirectories
image_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.includeExistingFiles", "true")  # Include existing files on the first run
    .option("cloudFiles.useIncrementalListing", "true")  # Enable incremental listing
    .schema(schema)
    .load("/mnt/s3/guardianvision/Guardian_Vision_Akshay/*")  # Use wildcard to include all subdirectories
)

# Write paths to Delta table incrementally with checkpointing
query = (
    image_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(delta_table_path)
)
