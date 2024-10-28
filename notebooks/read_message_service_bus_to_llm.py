# Databricks notebook source
# MAGIC %run ../notebooks/Utility

# COMMAND ----------

from azure.servicebus import ServiceBusClient
import json

# Azure Service Bus configuration
connection_str = "Endpoint=sb://guardianvision.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=D7kHlQiQGONEDZ6J3h7WKJk0hqJp0C3HZ+ASbGjQt/c="  # Replace with your connection string
queue_name = "guardianvision"  # Replace with your queue name


# Function to receive and process messages from the Azure Service Bus queue
def receive_and_process_messages(analyzer):
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_str, logging_enable=True)

    with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name=queue_name)

        with receiver:
            while True:
                messages = receiver.receive_messages(max_message_count=10, max_wait_time=5)
                if not messages:
                    break  # Exit the loop if no more messages are available

                for msg in messages:
                    message_body = json.loads(str(msg))
                    image_path = message_body.get("filepath")

                    if image_path:
                        try:
                            

                          # Ensure the image path exists in the message
                            prompt = "Basis the given checklist, assign a safety score from 1 to 5. Only look for obvious signs of threats, some of the items may not be visible in the image. Judge only basis what you see. Your output should be strict python list [safety rating, one sentence description]."
                            image_path = image_path.replace("dbfs:", "/dbfs/", 1)
                            analyzer_instance.analyze_image(image_path,prompt)
                        except Exception as e:
                            pass

                        # Complete the message
                        receiver.complete_message(msg)

    print("Queue is empty.")

# Create an instance of GuardianVisionAnalyzer
analyzer_instance = GuardianVisionAnalyzer(api_base_url="https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints")    
# Start processing messages
receive_and_process_messages(analyzer_instance)


# COMMAND ----------

analyzer_instance = GuardianVisionAnalyzer(api_base_url="https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints") 

# COMMAND ----------

dbutils.fs.ls('/mnt/s3/guardianvision/Guardian_Vision_Akshay/Camera_02_Site_01/How-To-Prevent-Your-Workers-From-Facing-Unsafe-Working-Conditions.jpg')
