# Databricks notebook source
from openai import OpenAI
import os

# COMMAND ----------

import base64

# Function to open and encode the image as a base64 string
def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")

# Define constants
#IMAGE_PATH = "path/to/your/image.png"  # Replace with the path to your image file
# Path to the image you want to analyze
IMAGE_PATH = "/Workspace/Users/rahul.jain@pumaenergy.com/guardianvision/test_data_construction/unsafe/002564baec48136553cf02.jpg"


# Encode the image
base64_image = encode_image(IMAGE_PATH)

# Initialize Databricks token and OpenAI client
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url="https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints"
)



# COMMAND ----------

# Send request with image and text
response = client.chat.completions.create(
    model="gpt4o",
    messages=[
        {"role": "system", "content": "You are a helpful assistant that reads image!"},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "What's in the image?"},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{base64_image}"}}
            ]
        }
    ],
    temperature=0.0,
)

# Print the assistant's response
print(response.choices[0].message.content)

# COMMAND ----------


