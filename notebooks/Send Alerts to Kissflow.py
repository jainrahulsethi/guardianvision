# Databricks notebook source
df = spark.sql(
'''
select * from (
SELECT 
    T1.site_id AS `Site ID`,
    T1.cam_id AS `Camera ID`,
    T2.Country AS Country,
    T2.site_name AS Site_Name,
    T2.site_manager_email AS Responsibility,
    T1.description AS Alert_Description,
    T1.safety_violation_category AS Alert_Type,
    T1.safety_rating AS `Safety_Rating`,
    T2.client_id AS `Client ID`,
    T1.image_path,
    ROW_NUMBER() OVER (PARTITION BY T1.site_id, T1.cam_id, T1.client_id,T1.safety_violation_category ORDER BY T1.Last_Updated_On DESC) AS row_num
FROM 
    guardianvision.safety_assessment_log T1
JOIN 
    guardianvision.client_data T2
ON 
    T1.client_id = T2.client_id
    AND T1.cam_id = T2.cam_id
    AND T1.site_id = T2.site_id
WHERE 
    T1.safety_rating < 3 and T1.IsProcessed is null) where row_num  =1 
'''
).toPandas()


# COMMAND ----------

import requests
import json
import pandas as pd
import os

# Define your access credentials
access_id = dbutils.secrets.get(scope="guardian_connection_str", key="kf_access_id")
secret_id = dbutils.secrets.get(scope="guardian_connection_str", key="kf_secret_id")

# Example DataFrame with column names matching your JSON payload requirements
# Replace this with the actual loading of your DataFrame

if not df.empty:
    # Loop through each row in the DataFrame
    for index, row in df.iterrows():
        # Prepare initial payload
        payload = {
            "Country": row["Country"],
            "Site_ID" :row["Site ID"],
            "Client_ID": row["Client ID"],
            "Camera_ID":row["Camera ID"],
            "Safety_Rating":row["Safety_Rating"],
            "Site_Name": row["Site_Name"],
            "Responsibility": row["Responsibility"],
            "Alert_Description": row["Alert_Description"],
            "Alert_Type": row["Alert_Type"]
        }
        
        # Initial request URL
        url = "https://pumaenergy.kissflow.eu/process/2/AcuVVWae50gm/GuardianVision"
        headers = {
            'X-Access-Key-Id': access_id,
            'X-Access-Key-Secret': secret_id,
            'Content-Type': 'application/json'
        }
        
        # Send POST request and get JSON response
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response_data = response.json()
        _id = response_data.get('_id')
        _activity_instance_id = response_data.get('_activity_instance_id')
        
        # Check if the response contains the required fields
        if not _id or not _activity_instance_id:
            print(f"Missing data in response for row {index}. Skipping...")
            continue

        # Extract image_name from image_path
        image_path = row['image_path']
        image_name = os.path.basename(image_path)

        # Second request: Upload image
        url_image_upload = f"https://pumaenergy.kissflow.eu/process/2/AcuVVWae50gm/GuardianVision/{_id}/{_activity_instance_id}/Alert_Snapshot/image"
        files = [
            ('file', (image_name, open(image_path, 'rb'), 'application/octet-stream'))
        ]
        headers_image = {
            'X-Access-Key-Id': access_id,
            'X-Access-Key-Secret': secret_id
        }
        
        response_image = requests.post(url_image_upload, headers=headers_image, files=files)
        print("Image Upload Response:", response_image.text)

        # Third request: Submit the final form
        url_submit = f"https://pumaenergy.kissflow.eu/process/2/AcuVVWae50gm/GuardianVision/{_id}/{_activity_instance_id}/submit"
        headers_submit = {
            'X-Access-Key-Id': access_id,
            'X-Access-Key-Secret': secret_id,
            'Content-Type': 'application/json'
        }
        
        response_submit = requests.post(url_submit, headers=headers_submit, data=json.dumps(payload))
        print("Submit Response:", response_submit.text)
else:
    print("No alerts to send")

# COMMAND ----------

try:

    spark_df = spark.createDataFrame(df)
    spark_df.createTempView('IsProcessedrows')
except:
    pass

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO guardianvision.safety_assessment_log AS t
# MAGIC USING (select distinct `Site ID`,`Camera ID`,`Client ID` from IsProcessedrows) AS I
# MAGIC ON t.site_id = I.`Site ID`
# MAGIC     AND t.cam_id = I.`Camera ID`
# MAGIC     AND t.client_id = I.`Client ID`
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET t.IsProcessed = 'yes';
