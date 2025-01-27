import logging
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from src.env_config import *
from src.api_data_pull import get_latest_file_from_folder, get_second_latest_file_from_folder
from datetime import datetime
from prefect import task

@task
def upload_data_to_azure_storage():
    
    logging.getLogger('azure').setLevel(logging.WARNING)
    today = datetime.today().strftime("%d-%m-%Y")
    blob_name = f'Classlink/Data/classlink_data_{today}.csv'
    file_path = get_latest_file_from_folder(CLASSLINK_INCREMENTAL_DATA, file_prefix='classlink_data_')
    # Initialize Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER) 

    # Check if container exists
    if not container_client.exists():
        logging.error(f"The container {CONTAINER} does not exist.")   
    else:
        logging.info("Container exists.")

        # Get blob client
        blob_client = container_client.get_blob_client(blob_name)

        # Delete the existing blob if it exists
        try:
            blob_client.delete_blob()
            logging.info(f"Blob '{blob_name}' deleted successfully.")
        except ResourceNotFoundError:
            logging.warning(f"Blob '{blob_name}' does not exist. Proceeding to upload.")

    # Upload the new blob
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)
            logging.info(f"File '{file_path}' uploaded to container '{CONTAINER}' as blob '{blob_name}'.")
    
@task
def upload_error_log_to_azure_storage():
    
    logging.getLogger('azure').setLevel(logging.WARNING)
    today = datetime.today().strftime("%d-%m-%Y")
    blob_name = f'Classlink/Errors/classlink_error_log_{today}.csv'
    try:
        file_path = get_latest_file_from_folder(folder_path=CLASSLINK_ERROR_LOGS, file_prefix='classlink_error_log_')
    except Exception as e:
        logging.info(f"No error log file found for today ({today}). Skipping upload. Error: {e}")
        return
    # Initialize Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER)

    # Check if container exists
    if not container_client.exists():
        logging.error(f"The container {CONTAINER} does not exist.")   
    else:
        logging.info("Container exists.")

        # Get blob client
        blob_client = container_client.get_blob_client(blob_name)

        # Delete the existing blob if it exists
        try:
            blob_client.delete_blob()
            logging.info(f"Blob '{blob_name}' deleted successfully.")
        except ResourceNotFoundError:
            logging.warning(f"Blob '{blob_name}' does not exist. Proceeding to upload.")

    # Upload the new blob
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)
            logging.info(f"File '{file_path}' uploaded to container '{CONTAINER}' as blob '{blob_name}'.")
        
         