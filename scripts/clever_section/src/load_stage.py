import logging
import sys
import numpy as np
import pandas as pd
from datetime import datetime
import datetime
import os
from src.env_config import *
sys.path.append(ROOT_DIR)
from scripts.oracledb_connection.internals.db import *
from datetime import datetime
from prefect import flow, task



timestamp = datetime.now().strftime("%d-%m-%Y")




@task
def lazy_load_clever_sections_df_into_db(
    file_path: str,
    table_name: str,
    schema: str = None,
    chunksize: int = 1000,
    db_dtype: dict = None,
    if_exists="fail",
    dtype: dict = None,
    parse_dates=None,
    ##method="multi",
) -> None:
    """
    Lazily load in a dataframe and pass in the chunks to the 'clever_sections' table,
    excluding 'uri' and 'data.teachers' columns if present.
    """

    for idx, file_chunk in enumerate(
        pd.read_csv(
            file_path,
            chunksize=chunksize,
            dtype={
                "data.course": "str",
                "data.created": "str",
                "data.district": "str",
                "data.grade": "str",
                "data.last_modified": "str",
                "data.name": "str",
                "data.period": "str",
                "data.school": "str",
                "data.section_number": "str",
                "data.sis_id": "str",
                "data.students": "str",
                "data.subject": "str",
                "data.teacher": "str",
                "data.term_id": "str",
                "data.id": "str",
            },  # Using pandas data types
        )
    ):
        



        file_chunk.replace(np.nan, None, inplace=True)

        # Rename columns to match the database table's column names
        file_chunk.rename(
            columns={
                "data.course": "course",
                "data.created": "created",
                "data.grade": "grade",
                "data.district": "district",
                "data.last_modified": "last_modified",
                "data.name": "name",
                "data.period": "period",
                "data.school": "school",
                "data.section_number": "section_number",
                "data.sis_id": "sis_id",
                "data.students": "students",
                "data.subject": "subject",
                "data.teacher": "teacher",
                "data.term_id": "term_id",
                "data.id": "id",
            },
            inplace=True,
        )

        # Exclude 'uri' and 'data.teachers' columns if present
        for col in ["uri", "data.teachers"]:
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
                ##method=method,
            )
            logging.info("Completed round.")
        except Exception as e:
            logging.error(f"Could not load data for round {idx} because: {e}")
            



@task
def truncate_staging_table():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_CLEVER_CLASSLINK_DATA_LOAD_PKG.pc_clever_sections_trunc", "x_load_Status")
    

@task
def run_clever_section_d_procedure():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_CLEVER_CLASSLINK_DATA_LOAD_PKG.pc_clever_sections_d", "x_load_Status")





