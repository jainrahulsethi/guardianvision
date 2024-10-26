# Databricks notebook source
# MAGIC %pip install -qqqq -U databricks-vectorsearch 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from mlflow.utils import databricks_utils as du
# The name of the RAG application.  This is used to name the chain's UC model and prepended to the output Delta Tables + Vector Indexes
RAG_APP_NAME = 'guardianvisionapp'

# UC Catalog & Schema where outputs tables/indexs are saved
# If this catalog/schema does not exist, you need create catalog/schema permissions.
UC_CATALOG = f'genai_applications'
UC_SCHEMA = f'guardianvision'

## UC Model name where the POC chain is logged
UC_MODEL_NAME = f"{UC_CATALOG}.{UC_SCHEMA}.{RAG_APP_NAME}"

# Vector Search endpoint where index is loaded
# If this does not exist, it will be created
# Names of the output Delta Tables tables & Vector Search index
destination_tables_config = {
    # Staging table with the raw files & metadata
    "raw_files_table_name": f"{UC_CATALOG}.{UC_SCHEMA}.{RAG_APP_NAME}_poc_raw_files_bronze",
    # Parsed documents
    "parsed_docs_table_name": f"{UC_CATALOG}.{UC_SCHEMA}.{RAG_APP_NAME}_poc_parsed_docs_silver",
    # Chunked documents that are loaded into the Vector Index
    "chunked_docs_table_name": f"{UC_CATALOG}.{UC_SCHEMA}.{RAG_APP_NAME}_poc_chunked_docs_gold",
    # Destination Vector Index
    "vectorsearch_index_name": f"{UC_CATALOG}.{UC_SCHEMA}.{RAG_APP_NAME}_poc_chunked_docs_gold_index",
}

def get_table_url(table_fqdn):
    split = table_fqdn.split(".")
    browser_url = du.get_browser_hostname()
    url = f"https://{browser_url}/explore/data/{split[0]}/{split[1]}/{split[2]}"
    return url

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# Get the vector search index
vsc = VectorSearchClient(disable_notice=True)

VECTOR_SEARCH_ENDPOINT = 'guardianvision'

# COMMAND ----------

index = vsc.get_index(endpoint_name=VECTOR_SEARCH_ENDPOINT, index_name=destination_tables_config["vectorsearch_index_name"])

print(f'Vector index: {get_table_url(destination_tables_config["vectorsearch_index_name"])}')
print("\nOutput tables:\n")
print(f"Bronze Delta Table w/ raw files: {get_table_url(destination_tables_config['raw_files_table_name'])}")
print(f"Silver Delta Table w/ parsed files: {get_table_url(destination_tables_config['parsed_docs_table_name'])}")
print(f"Gold Delta Table w/ chunked files: {get_table_url(destination_tables_config['chunked_docs_table_name'])}")

# COMMAND ----------

result_set = index.similarity_search(columns=["chunked_text", "chunk_id", "path"], query_text="Safety checklist for Construction activity with scaffold and heavy machinery operation")

# COMMAND ----------

result_set

# COMMAND ----------

result_set.keys()

# COMMAND ----------

print(result_set['result']['data_array'])

# COMMAND ----------

print(result_set['result']['data_array'][0][0])

# COMMAND ----------


