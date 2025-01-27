from prefect import flow
from src.env_config import *
from src.api_data_pull import *
from src.load import *
from src.azure_upload import *


@flow
def classlink_flow():
    try: 
        run_classes_data_pull()
        get_incremental_data()
        truncate_staging_table()
        lazy_load_data_to_db(
            file_path=get_latest_file_from_folder(folder_path=CLASSLINK_INCREMENTAL_DATA,file_prefix='classlink_data_'), 
            table_name="FEV_CLASSLINK_CLASS_STG", 
            schema="DWH_STAGING",  
            chunksize=10000,  
            if_exists="append",  
            db_dtype=None,  
        )
        run_classes_d_procedure()
        run_classes_f_procedure()
        upload_data_to_azure_storage()
        upload_error_log_to_azure_storage()
        delete_old_files()

    except Exception as e:
        send_email(
            subject=f"{REGION}Classlink ETL failure",
            msg=f"An error occurred during the {REGION} Classlink ETL run: {str(e)}",
            email_to=RECEIVERS_EMAIL
        )

if __name__ == "__main__":
    setup_logging()
    classlink_flow()    
    









