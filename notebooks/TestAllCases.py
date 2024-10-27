# Databricks notebook source
# MAGIC %pip install -qqqq -U databricks-vectorsearch 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from mlflow.utils import databricks_utils as du
from databricks.vector_search.client import VectorSearchClient

class GuardianVisionRAG:
    """
    GuardianVisionRAG is a class designed for performing RAG searches using Databricks Vector Search.
    """

    def __init__(self, app_name='guardianvisionapp', catalog='genai_applications', schema='guardianvision', vector_search_endpoint='guardianvision'):
        """
        Initializes the RAG application with the required configuration.

        Parameters:
            app_name (str): Name of the RAG application.
            catalog (str): UC Catalog where output tables and indexes are saved.
            schema (str): UC Schema where output tables and indexes are saved.
            vector_search_endpoint (str): The endpoint name for the vector search.
        """
        self.app_name = app_name
        self.catalog = catalog
        self.schema = schema
        self.vector_search_endpoint = vector_search_endpoint

        # Configuration for output Delta Tables and Vector Search Index
        self.destination_tables_config = {
            "raw_files_table_name": f"{self.catalog}.{self.schema}.{self.app_name}_poc_raw_files_bronze",
            "parsed_docs_table_name": f"{self.catalog}.{self.schema}.{self.app_name}_poc_parsed_docs_silver",
            "chunked_docs_table_name": f"{self.catalog}.{self.schema}.{self.app_name}_poc_chunked_docs_gold",
            "vectorsearch_index_name": f"{self.catalog}.{self.schema}.{self.app_name}_poc_chunked_docs_gold_index",
        }

        # Initialize Vector Search Client
        self.vsc = VectorSearchClient(disable_notice=True)

    def get_table_url(self, table_fqdn):
        """
        Constructs the Databricks URL for the given table FQDN.

        Parameters:
            table_fqdn (str): Fully qualified domain name of the table.

        Returns:
            str: URL to access the table in Databricks.
        """
        split = table_fqdn.split(".")
        browser_url = du.get_browser_hostname()
        return f"https://{browser_url}/explore/data/{split[0]}/{split[1]}/{split[2]}"

    def perform_search(self, query_text):
        """
        Performs a similarity search using the provided query text.

        Parameters:
            query_text (str): The text query to search for.

        Returns:
            str: The top result from the similarity search or a message indicating no results found.
        """
        # Retrieve the vector search index
        index = self.vsc.get_index(
            endpoint_name=self.vector_search_endpoint, 
            index_name=self.destination_tables_config["vectorsearch_index_name"]
        )

        # Perform a similarity search
        result_set = index.similarity_search(
            columns=["chunked_text", "chunk_id", "path"], 
            query_text=query_text
        )

        # Return the top result if available
        if result_set and result_set['result']['data_array']:
            return result_set['result']['data_array'][0][0]
        else:
            return "None"

# COMMAND ----------

import base64
from openai import OpenAI
import os

class GuardianVisionAnalyzer:
    """
    GuardianVisionAnalyzer is a class designed to analyze images using OpenAI API on Databricks.
    """

    def __init__(self, api_base_url):
        """
        Initializes the analyzer with the necessary configuration.
        
        Parameters:
            api_base_url (str): The base URL for the OpenAI API endpoint.
        """
        self.api_base_url = api_base_url
        self.databricks_token = self._get_databricks_token()
        self.client = OpenAI(
            api_key=self.databricks_token,
            base_url=self.api_base_url
        )

    def _get_databricks_token(self):
        """
        Retrieves the Databricks API token.
        
        Returns:
            str: The Databricks API token.
        """
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    def _encode_image(self, image_path):
        """
        Encodes the image as a base64 string.
        
        Parameters:
            image_path (str): Path to the image file.
        
        Returns:
            str: The base64 encoded string of the image.
        """
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")

    def analyze_image(self, image_path, prompt):
        """
        Analyzes the image using OpenAI's GPT model and returns the response based on the provided prompt.
        
        Parameters:
            image_path (str): Path to the image file to be analyzed.
            prompt (str): The user-defined prompt/question to query the image.
        
        Returns:
            str: The response content from the OpenAI model.
        """
        base64_image = self._encode_image(image_path)
        
        # Send request with image and dynamic text prompt
        response = self.client.chat.completions.create(
            model="gpt4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that reads images!"},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{base64_image}"}}
                    ]
                }
            ],
            temperature=0.0,
        )

        return response.choices[0].message.content


# COMMAND ----------

import os

#STEP 1:- First analyze the RAG search to find out the correct safety checklist
rag_search = GuardianVisionRAG()
query_text = "Safety checklist for Construction activity with scaffold and heavy machinery operation"
result = rag_search.perform_search(query_text)
#print("Search Result:\n", result)

#STEP 2:- In case the result is not found, then use another LLM to find out the correct safety checklist basis LLM
#TBD

#STEP 3:- Use output from Step 1 to form a correct prompt and pass it to the Image Analyzer

image_analyzer = GuardianVisionAnalyzer(
    api_base_url="https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints"
)
image_path = "/Workspace/Users/rahul.jain@pumaenergy.com/guardianvision/test_data_construction/unsafe/002564baec48136553cf02.jpg"
prompt = "Basis the given checklist, assign a safety score from 1 to 5. Only look for obvious signs of threats, some of the items may not be visible in the image. Judge only basis what you see. Your output should be strict python list [safety rating, one sentence description]. Here is the checklist:- " +result

result = image_analyzer.analyze_image(image_path, prompt)
print("Analysis Result:\n", result)

# Initialize the GuardianVisionAnalyzer
image_analyzer = GuardianVisionAnalyzer(
    api_base_url="https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints"
)

# Directory containing the images
directory_path = "/Workspace/Users/rahul.jain@pumaenergy.com/guardianvision/test_data_construction/unsafe"

# Prompt to be used for analysis
base_prompt = (
    "Basis the given checklist, assign a safety score from 1 to 5. Only look for obvious signs of threats, "
    "some of the items may not be visible in the image. Judge only basis what you see. "
    "Your output should be a strict python list [safety rating, one sentence description]. "
    "Here is the checklist:- " + result
)

# Dictionary to store the results
results_dict = {}

# Iterate through all files in the directory
for filename in os.listdir(directory_path):
    if filename.endswith((".jpg", ".png", ".jpeg")):  # Filter for image files
        image_path = os.path.join(directory_path, filename)
        try:
            # Analyze the image using the dynamic prompt
            result = image_analyzer.analyze_image(image_path, base_prompt)
            # Store the result in the dictionary with the image name as the key
            results_dict[filename] = result
        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Print the results
for image_name, analysis_result in results_dict.items():
    print(f"Image: {image_name}, Analysis Result: {analysis_result}")



# COMMAND ----------

print(results_dict)

# COMMAND ----------

import os
# Example usage
if __name__ == "__main__":

    #STEP 1:- First analyze the RAG search to find out the correct safety checklist
    rag_search = GuardianVisionRAG()
    query_text = "Safety checklist for Construction activity with scaffold and heavy machinery operation"
    result = rag_search.perform_search(query_text)
    #print("Search Result:\n", result)

    #STEP 2:- In case the result is not found, then use another LLM to find out the correct safety checklist basis LLM
    #TBD

    #STEP 3:- Use output from Step 1 to form a correct prompt and pass it to the Image Analyzer

    image_analyzer = GuardianVisionAnalyzer(
        api_base_url="https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints"
    )
    image_path = "/Workspace/Users/rahul.jain@pumaenergy.com/guardianvision/test_data_construction/unsafe/002564baec48136553cf02.jpg"
    prompt = "Basis the given checklist, assign a safety score from 1 to 5. Only look for obvious signs of threats, some of the items may not be visible in the image. Judge only basis what you see. Your output should be strict python list [safety rating, one sentence description]. Here is the checklist:- " +result

    result = image_analyzer.analyze_image(image_path, prompt)
    print("Analysis Result:\n", result)

    # Initialize the GuardianVisionAnalyzer
    image_analyzer = GuardianVisionAnalyzer(
        api_base_url="https://adb-3971841089204274.14.azuredatabricks.net/serving-endpoints"
    )

    # Directory containing the images
    directory_path = "/Workspace/Users/rahul.jain@pumaenergy.com/guardianvision/test_data_construction/unsafe"

    # Prompt to be used for analysis
    base_prompt = (
        "Basis the given checklist, assign a safety score from 1 to 5. Only look for obvious signs of threats, "
        "some of the items may not be visible in the image. Judge only basis what you see. "
        "Your output should be a strict python list [safety rating, one sentence description]. "
        "Here is the checklist:- " + result
    )

    # Dictionary to store the results
    results_dict = {}

    # Iterate through all files in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith((".jpg", ".png", ".jpeg")):  # Filter for image files
            image_path = os.path.join(directory_path, filename)
            try:
                # Analyze the image using the dynamic prompt
                result = image_analyzer.analyze_image(image_path, base_prompt)
                # Store the result in the dictionary with the image name as the key
                results_dict[filename] = result
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    # Print the results
    for image_name, analysis_result in results_dict.items():
        print(f"Image: {image_name}, Analysis Result: {analysis_result}")



# COMMAND ----------


