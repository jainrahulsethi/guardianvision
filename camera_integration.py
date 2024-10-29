# Import necessary libraries
import cv2
import numpy as np
import io
import datetime
import time
import os
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Email, Mail, Attachment, FileContent, FileName, FileType, Disposition

# Define the RTSP stream URL
rtsp_url = 'test_rtsp_path_to_come_here'

# Initialize the last sent email and saved image time
lastEmailSentTime = None
lastImageSavedTime = None

def should_save_image():
    """Check if enough time has passed to save a new image (5 minutes)."""
    global lastImageSavedTime
    current_time = datetime.datetime.now()
    
    if lastImageSavedTime is None or (current_time - lastImageSavedTime) > datetime.timedelta(minutes=5):
        lastImageSavedTime = current_time
        return True
    return False

def should_send_email():
    """Check if enough time has passed to send a new email alert (2 hours)."""
    global lastEmailSentTime
    current_time = datetime.datetime.now()
    
    if lastEmailSentTime is None or (current_time - lastEmailSentTime) > datetime.timedelta(hours=2):
        lastEmailSentTime = current_time
        return True
    return False

def send_email_with_attachment(sendgrid_api_key, from_email, to_emails, subject, content, attachment_path):
    """Send an email with an attachment using SendGrid."""
    message = Mail(from_email=from_email, to_emails=to_emails, subject=subject, html_content=content)
    
    with open(attachment_path, 'rb') as f:
        data = f.read()
        encoded_file = base64.b64encode(data).decode()
    
    attachment = Attachment(
        file_content=FileContent(encoded_file),
        file_type=FileType('image/jpeg'),
        file_name=FileName('attachment.jpg'),
        disposition=Disposition('attachment')
    )
    
    message.attachment = attachment
    
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print(f"Email sent with status code: {response.status_code}")
    except Exception as e:
        print(f"Error sending email: {str(e)}")

# Placeholder function for safety violation identification
def identify_safety_violations(frame):
    """Placeholder function to identify safety violations in the frame."""
    # Placeholder logic to identify safety violations
    # Return True if safety violation is detected, False otherwise
    # In the future, integrate your model here
    isViolationDetected = False  # Set this based on your logic or model
    
    # Example logic (you can replace this with actual detection logic later):
    if np.random.rand() > 0.5:  # Random detection for placeholder purposes
        isViolationDetected = True
    
    return isViolationDetected

def processFrameAndAlert(frame):
    """Process a frame, check for safety violations, and send alerts if necessary."""
    isViolationDetected = identify_safety_violations(frame)

    if isViolationDetected:
        exception_to_send = "Safety violations such as no hard hat found"
        
        if should_save_image():
            _, buffer = cv2.imencode('.jpg', frame)
            image_stream = io.BytesIO(buffer)
            current_time = datetime.datetime.now()
            image_path = f"/dbfs/mnt/camerasurveillance/imagesdump/{current_time}.jpg"
            
            with open(image_path, "wb") as f:
                f.write(image_stream.getbuffer())
        
        if should_send_email():
            _, buffer = cv2.imencode('.jpg', frame)
            image_stream = io.BytesIO(buffer)
            attachment_path = f"/dbfs/mnt/camerasurveillance/exceptions/{datetime.datetime.now()}.jpg"
            
            with open(attachment_path, "wb") as f:
                f.write(image_stream.getbuffer())
            
            email_content = (
                f"Alert! Attached unsafe behavior noticed at Pilot site on {datetime.datetime.now()}.\n"
                f"Exception Type: {exception_to_send}"
            )
            send_email_with_attachment(sendgrid_api_key, from_email, to_emails, subject, email_content, attachment_path)
            return True

    return False

def attempt_connection(rtsp_url):
    """Attempt to connect to the RTSP stream with retries."""
    attempts = 0
    while attempts < 3:
        cap = cv2.VideoCapture(rtsp_url)
        if cap.isOpened():
            return cap
        print(f"Attempt {attempts + 1} failed. Retrying in 5 seconds...")
        time.sleep(5)
        attempts += 1
    raise Exception("Error: Camera is not available")

# Main loop to capture frames and process them
total_exceptions = 0

while True:
    try:
        cap = attempt_connection(rtsp_url)
        ret, frame = cap.read()
        
        if not ret:
            print("Error: Can't receive frame. Reconnecting...")
            cap.release()
            time.sleep(3)
            continue
        
        if processFrameAndAlert(frame):
            total_exceptions += 1
        
        cap.release()
        print("Sleeping for 20 seconds before fetching a fresh frame...")
        time.sleep(20)
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        break

cv2.destroyAllWindows()
