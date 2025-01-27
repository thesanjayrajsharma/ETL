from src.env_config import *
from src.api_data import *
from src.load_stage import *
from prefect import flow



@flow
def userpilot_flow():
    blob_data = data_load()
    truncate_staging_table()
    lazy_load_userpilot_df_into_db(
        file_path=blob_data,
        table_name="FEV_USERPILOT_STG",
        schema="DWH_STAGING",
        chunksize=2500,
        if_exists="append",
        db_dtype=None,
    )
    run_clever_courses_d_procedure()
    
if __name__ == "__main__":
    userpilot_flow()