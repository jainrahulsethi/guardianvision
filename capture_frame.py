# Databricks notebook source
import cv2
import sys
import time
from datetime import datetime
import tempfile 
from azure.storage.blob import BlobServiceClient
import os

def save_to_dls (local_file, client_id, file_name):
    # Configuration
    account_name = "pumadls"
    account_key = "Ynyg+MXSryff8FBfZbzwdAJVyYK8cZtJRd1qDuYkQODD8/RJA6V/gocutY1V7eFSim17QVWIQXDOSUQqZHS3pg=="
    container_name = "retaildls"
    local_file_path = local_file  # Local path to the image file
    #blob_name = os.path.basename(local_file_path)  # Name of the blob in ADLS

    curr_date = datetime.now().date()
    blob_name = f"guardianvision/frame_data/{curr_date}/{client_id}/{file_name}"

    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
    )

    # Get a container client
    container_client = blob_service_client.get_container_client(container_name)

    # Upload the file
    try:
        with open(local_file_path, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data)
        print(f"Successfully uploaded {blob_name} to {container_name}.")
    except Exception as e:
        print(f"Error uploading file: {e}")

def capture_frame(rtsp_url, camera_id, client_id, site_id):
    try:
        cap = cv2.VideoCapture(rtsp_url)
        ret, frame = cap.read()
        cap.release()
        if ret:
            # Define the file path to save
            timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
            adls_base_path = "/mnt/retaildls/guardianvision"
            frame_path = f"/dbfs/tmp/guardianvision/frame_data/frame_{camera_id}_{timestamp}.jpg"
            cv2.imwrite(frame_path, frame)
            file_name = f"frame_{site_id}_{camera_id}_{timestamp}.jpg"
            save_to_dls(frame_path, client_id, file_name)

        else:
            print(f"Failed to capture frame from {rtsp_url}")
    except Exception as e:
        print(f"Error capturing frame: {e}")

# Read arguments passed by subprocess
if __name__== "__main__":
    rtsp_url = sys.argv[1]
    camera_id = sys.argv[2]
    client_id = sys.argv[3]
    site_id = sys.argv[4]
    capture_frame(rtsp_url, camera_id, client_id, site_id)
