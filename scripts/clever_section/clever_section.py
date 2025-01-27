from prefect import flow
from src.env_config import *
from src.api_data_pull import *
from src.load_stage import *
from src.azure_upload import upload_to_azure_storage


@flow
def section_flow():
    try:
        get_sections_data()
        get_incremental_data()
        truncate_staging_table()
        lazy_load_clever_sections_df_into_db(
            file_path=get_latest_file_from_folder(CLEVER_INCREMENTAL_DATA,file_prefix='Clever_sections_data_'),
            table_name="FEV_CLEVER_SECTION_STG",
            schema="DWH_STAGING", 
            chunksize=10000, 
            if_exists="append",
            db_dtype=None,
        )
        run_clever_section_d_procedure()
        upload_to_azure_storage()
        delete_old_files()
    except Exception as e:
        send_email(
            subject=f"{REGION} Clever_sections ETL failure",
            msg=f"An error occurred during the {REGION} clever sections ETL run: {str(e)}",
            email_to=RECEIVERS_EMAIL
        )



if __name__ == "__main__":
    setup_logging()
    section_flow()
    