import logging
import sys
import numpy as np
import pandas as pd
from datetime import datetime
from src.env_config import *
sys.path.append(ROOT_DIR)
from scripts.oracledb_connection.internals.db import * 
from io import StringIO
from prefect import flow, task

@task
def lazy_load_userpilot_df_into_db(
    file_path: str,
    table_name: str,
    schema: str = None,
    chunksize: int = 1000,
    db_dtype: dict = None,
    if_exists="fail",
    dtype: dict = None,
    parse_dates=None,
) -> None:

    file_path = StringIO(file_path)

    for idx, file_chunk in enumerate(
        pd.read_csv(
            file_path,
            chunksize=chunksize,
            dtype={
                "user_id": "str",
                "name": "str",
                "email": "str",
                "role_name": "str",
                "score": "str",
                "feedback": "str",
                "sessions": "str",
                "schoolId": "str",
                "school_name": "str",
                "district_name": "str",
                "hostname": "str",
                "timestamp_date":"str"
            },
        )
    ):
        
        file_chunk.replace(np.nan, None, inplace=True)

        file_chunk.rename(
            columns={
                "user_id": "user_id",
                "name": "name",
                "email": "email",
                "role_name": "role_name",
                "score": "score",
                "feedback": "feedback",
                "sessions": "sessions",
                "schoolId": "schoolid",
                "school_name": "school_name",
                "district_name": "district_name",
                "hostname": "hostname",
                "timestamp_date":"event_create_date"
                
            },
            inplace=True,
        )



        # Convert 'event_create_date' to datetime
        file_chunk['event_create_date'] = pd.to_datetime(file_chunk['event_create_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        
        try:
            insert_dataframe(
                table_name,
                file_chunk,
                schema=schema,
                if_exists=if_exists,
                chunksize=chunksize,
                dtype=db_dtype,
            )
            logging.info("Completed round.")
        except Exception as e:
            logging.error(f"Could not load data for round {idx} because: {e}")



@task
def truncate_staging_table():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_USERPILOT_DATA_LOAD_PKG.pc_userpilot_trunc", "x_load_Status")
    

@task
def run_clever_courses_d_procedure():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_USERPILOT_DATA_LOAD_PKG.pc_userpilot_d", "x_load_Status")

