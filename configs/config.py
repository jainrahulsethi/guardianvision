# Databricks notebook source
# Configuration Dictionary
config = {
    "abus": {
        "key": "your_api_key",
        "endpoint": "https://api.example.com/v1/",
        "timeout": 30  # Timeout in seconds
    },
    "adls": {
        "account_name": "pumadls",
        "container_name": "retaildls",
        "adls_base_path": "/mnt/retaildls/guardianvision"
    },
    "api": {
        "key": "your_api_key",
        "endpoint": "https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints",
        "timeout": 30  # Timeout in seconds
    },
    "gaurdianvision": {
        "checkpoint_loc": "/dbfs/tmp/gaurdianvision/temp_checkpoint",
        "query_text": "Safety checklist for construction activity with scaffolding and heavy machinery",
        "output_path": "output/",
        "prompt_prefix": (
            "Based on the provided checklist, assign a safety score from 1 to 5. Assess only what is visible in the image, "
            "and avoid penalizing for items that may not be captured due to camera limitations. For unsafe cases, the rating "
            "should be only 1 or 2 and for safe 4 or 5. Your response should be a JSON object with the following keys: "
            "safety_rating, safety_violation_category (optional, may be null if no violation is detected), "
            "and one_sentence_description. Here is the checklist: "
        )
    },
    "client": {
        "frame_extraction_freq": 30,
        "enable_feature_y": False
    }
}


# Accessing the Configuration Values
# print("ABUS Endpoint:", config["abus"]["endpoint"])
# print("ADLS Account Name:", config["adls"]["account_name"])
# print("Guardian Vision Checkpoint Location:", config["gaurdianvision"]["checkpoint_loc"])

