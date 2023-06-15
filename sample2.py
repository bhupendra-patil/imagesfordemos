# Databricks notebook source


# COMMAND ----------

# Import the required libraries
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient

# Set the credential object
credential = DefaultAzureCredential()

# Set the file system and file path
file_system_name = "<file_system_name>"
file_path = "<file_path>"

# Create a DataLakeFileClient object
file_client = DataLakeFileClient.from_connection_string("<connection-string>", file_system_name=file_system_name, file_path=file_path, credential=credential)

# Read the contents of the file
file_contents = file_client.read_file()

# Print the contents of the file
print(file_contents)
