# Databricks notebook source
# capture_frame.py
import cv2
import sys
import time
from datetime import datetime

def capture_frame(rtsp_url, camera_id):
    try:
        cap = cv2.VideoCapture(rtsp_url)
        ret, frame = cap.read()
        cap.release()
        if ret:
            # Define the file path to save
            frame_path = f"/dbfs/tmp/gaurdianvision/frame_data/frame_{camera_id}_{datetime.now()}.jpg"
            cv2.imwrite(frame_path, frame)
            print(f"Frame saved for camera {camera_id} at {frame_path}")
        else:
            print(f"Failed to capture frame from {rtsp_url}")
    except Exception as e:
        print(f"Error capturing frame: {e}")

# Read arguments passed by subprocess
if __name__== "__main__":  
    rtsp_url = sys.argv[1]
    camera_id = sys.argv[2]
    capture_frame(rtsp_url, camera_id)
