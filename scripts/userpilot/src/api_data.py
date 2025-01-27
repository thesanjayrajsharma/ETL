from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import sys
from src.env_config import *
from src.load_stage import *
sys.path.append(ROOT_DIR)
from datetime import datetime, timedelta
from prefect import flow, task

@task
def get_yesterdays_blob_name():

    yesterday = datetime.now() - timedelta(days=1)
    blob_name = f'userpilot_data_{yesterday.strftime("%Y%m%d")}.csv'
    return blob_name
    
@task
def data_load():
    
    blob_name = get_yesterdays_blob_name()
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            logging.info(f"The container {container_name} does not exist.")
            return

        blob_client = container_client.get_blob_client(blob_name)

        if not blob_client.exists():
            logging.info(f"The blob {blob_name} was not found in the container {container_name}.")
            return


        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()
        return blob_data.decode('utf-8')  


    except Exception as e:
       raise

