from datetime import datetime
import sys
import numpy as np
from sqlalchemy import VARCHAR, Float
from src.env_config import *
from src.etl import *
sys.path.append(ROOT_DIR)
from scripts.oracledb_connection.internals.db import *

@task
def load_student_data_to_staging():
    folder_path = STUDENT_DATA_PATH

    # Ensure the folder path exists
    if not os.path.exists(folder_path):
        raise ValueError(f"Folder path '{folder_path}' does not exist.")
    
    latest_file = get_latest_file_from_folder(folder_path, file_prefix='student_data_')

    if latest_file is None:
        raise ValueError("No file found with today's date.")

    lazy_load_df_into_db(
        file_path=latest_file,
        table_name="FEV_NWEA_STUDENT_STAGE",
        chunksize=10000,
        if_exists="append",
        schema="DWH_STAGING",
        db_dtype={
            "DATE_OF_BIRTH": VARCHAR(100),
            "DISTRICT_BID": VARCHAR(100),
            "ETHNICITY": VARCHAR(100),
            "ETHNICITY_CUSTOM": VARCHAR(100),
            "FIRST_NAME": VARCHAR(100),
            "GENDER": VARCHAR(100),
            "GRADE": VARCHAR(100),
            "GRADE_CUSTOM": VARCHAR(100),
            "LAST_NAME": VARCHAR(100),
            "MIDDLE_NAME": VARCHAR(100),
            "SCHOOL_BID": VARCHAR(100),
            "STATE_STUDENT_ID": VARCHAR(100),
            "STUDENT_BID": VARCHAR(100),
            "STUDENT_ID": VARCHAR(100)
        }
    )



def lazy_load_df_into_db(
    file_path: str,
    table_name: str,
    schema: str = None,
    chunksize: int = 2500,
    db_dtype: dict = None,
    if_exists="fail",
    dtype: dict = None,
    parse_dates=None,
) -> None:
    expected_columns = [
        "DATE_OF_BIRTH",
        "DISTRICT_BID",
        "ETHNICITY",
        "ETHNICITY_CUSTOM",
        "FIRST_NAME",
        "GENDER",
        "GRADE",
        "GRADE_CUSTOM",
        "LAST_NAME",
        "MIDDLE_NAME",
        "SCHOOL_BID",
        "STATE_STUDENT_ID",
        "STUDENT_BID",
        "STUDENT_ID"
    ]

    for idx, file_chunk in enumerate(
        pd.read_csv(
            file_path,
            chunksize=chunksize,
            dtype=dtype,
            parse_dates=parse_dates
        )
    ):
        # Add missing columns with None values
        for col in expected_columns:
            if col not in file_chunk.columns:
                file_chunk[col] = None

        file_chunk = file_chunk[expected_columns]  # Ensure the order of columns matches the expected order

        if idx % 10 == 0:
            logging.info(f"On round {idx}")

        
        insert_dataframe(
            table_name,
            file_chunk,
            schema=schema,
            if_exists=if_exists,
            chunksize=chunksize,
            dtype=db_dtype,
        )
        logging.info(f"Completed round {idx}") 
        
           
@task
def truncate_student_staging():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_NWEA_DIMENSION_DATA_LOAD_PKG.pc_trun_student_d","x_load_Status")


@task
def run_student_d_procedure():
        prepare_data_in_data_warehouse("DWH_DATA.FEV_NWEA_DIMENSION_DATA_LOAD_PKG.pc_nwea_student_d","x_load_Status")