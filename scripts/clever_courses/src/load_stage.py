import logging
import sys
import numpy as np
import pandas as pd
from datetime import datetime
from src.env_config import *
sys.path.append(ROOT_DIR)
from scripts.oracledb_connection.internals.db import * 
from prefect import flow, task


@task
def lazy_load_clever_coureses_df_into_db(
    file_path: str,
    table_name: str,
    schema: str = None,
    chunksize: int = 10000,
    db_dtype: dict = None,
    if_exists="fail",
    dtype: dict = None,
    parse_dates=None,
    #method="multi",
) -> None:
    """
    Lazily load in a dataframe and pass in the chunks to the 'employee' table,
    excluding 'url' and 'uri' columns if present.
    """
    
    for idx, file_chunk in enumerate(
        pd.read_csv(
            file_path,
            chunksize=chunksize,
            dtype={
                "data.district": "str",
                "data.name": "str",
                "data.number": "str",
                "data.id": "str"
            }  # Using pandas data types
        )
    ):
        file_chunk.replace(np.nan, None, inplace=True)

        # Rename columns to match the database table's column names
        file_chunk.rename(columns={
            "data.district": "district",
            "data.name": "course_name",
            "data.number": "course_number",
            "data.id": "course_id"
        }, inplace=True)
        
        # Exclude 'url' and 'uri' columns if present
        for col in ['url', 'uri']:
            if col in file_chunk.columns:
                file_chunk.drop(col, axis=1, inplace=True)


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
    prepare_data_in_data_warehouse("DWH_DATA.FEV_CLEVER_CLASSLINK_DATA_LOAD_PKG.pc_clever_courses_trunc", "x_load_Status")
    

@task
def run_clever_courses_d_procedure():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_CLEVER_CLASSLINK_DATA_LOAD_PKG.pc_clever_courses_d", "x_load_Status")


