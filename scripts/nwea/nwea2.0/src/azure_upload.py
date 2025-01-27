import logging
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from src.env_config import *
from src.etl import get_latest_file_from_folder
from datetime import datetime
from prefect import flow, task


def upload_to_azure_storage(folder_path,file_prefix,blob_folder,blob_file_prefix):
    
    logging.getLogger('azure').setLevel(logging.WARNING)
    today = datetime.today().strftime("%d-%m-%Y")
    blob_name = f'{blob_folder}/{blob_file_prefix}_{today}.csv'
    try:
        file_path = get_latest_file_from_folder(folder_path=folder_path, file_prefix=file_prefix)
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
def upload_assessment_data_to_azure():
    upload_to_azure_storage(folder_path=INCREMENTAL_DATA_FILE_PATH, file_prefix='assessment_results_incremental_', blob_folder=ASSESSMENT_DATA_BLOB_FOLDER, blob_file_prefix='assessment_results_') 
@task  
def upload_assessment_errors_to_azure():
    upload_to_azure_storage(folder_path=ERRORS_FILE_PATH, file_prefix='final_errors_', blob_folder=ASSESSMENT_ERRORS_BLOB_FOLDER, blob_file_prefix='assessment_results_errors_')
@task  
def upload_assessment_metadata_to_azure():
    upload_to_azure_storage(folder_path=META_DATA_FILE_PATH, file_prefix='meta_data_', blob_folder=ASSESSMENT_METADATA_BLOB_FOLDER, blob_file_prefix='meta_data_')
@task 
def upload_student_data_to_azure():
    upload_to_azure_storage(folder_path=STUDENT_DATA_PATH,file_prefix='student_data_',blob_folder=STUDENT_DATA_BLOB_FOLDER,blob_file_prefix='student_data_')
@task
def upload_student_errors_to_azure():
    upload_to_azure_storage(folder_path=STUDENT_ERRORS_PATH, file_prefix='student_errors_', blob_folder=STUDENT_ERRORS_BLOB_FOLDER, blob_file_prefix='student_errors_')
    

@flow
def upload_files_to_azure_storage():
    upload_assessment_data_to_azure()
    upload_assessment_errors_to_azure()
    upload_assessment_metadata_to_azure()
    upload_student_data_to_azure()
    upload_student_errors_to_azure()