# Databricks notebook source
# Messaging Queue Config
abus:
  key: "your_api_key"
  endpoint: "https://api.example.com/v1/"
  timeout: 30  # Timeout in seconds

# ADlS Config
adls:
  account_name: "pumadls"
  container_name: "retaildls"
  adls_base_path: "/mnt/retaildls/guardianvision"

# App apis (alerts)

# LLM Configuration
api:
  key: "your_api_key"
  endpoint: "https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints"
  timeout: 30  # Timeout in seconds

# # Logging Configuration
# logging:
#   level: "INFO"
#   log_file: "logs/app.log"
#   max_size: 10MB  # Max size for the log file
#   backup_count: 5 # Number of backup log files

# General Project Parameters
gaurdianvision:
  checkpoint_loc  = "/dbfs/tmp/gaurdianvision/temp_checkpoint"
  query_text : "Safety checklist for construction activity with scafolding and heavy machinery" 
  output_path: "output/"
  prompt_prefix : "Based on the provided checklist, assign a safety score from 1 to 5. Assess only what is visible in the image, and avoid penalizing for items that may not be captured due to camera limitations. For unsafe cases, the rating should be only 1 or 2 and for safe 4 or 5. Your response should be a JSON object with the following keys: safety_rating, safety_violation_category (optional, may be null if no violation is detected), and one_sentence_description. Here is the checklist: "
  
# User control config
client:
  frame_extraction_freq: 30
  enable_feature_y: false

