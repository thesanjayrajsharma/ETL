import sys
import numpy as np
from prefect import task
from src.env_config import ROOT_DIR
import logging
sys.path.append(ROOT_DIR)
from scripts.oracledb_connection.internals.db import *




@task
def lazy_load_data_to_db(
    file_path: str,
    table_name: str,
    schema: str = None,
    chunksize: int = 2000,
    db_dtype: dict = None,
    if_exists="append",
    dtype: dict = None,
    parse_dates=None,
   # method="multi",
) -> None:
    """
   
    """

    for idx, file_chunk in enumerate(
        pd.read_csv(
            file_path,
            chunksize=chunksize,
            dtype={
                "sourcedId": "str",
                "status": "str",
                "title": "str",
                "dateLastModified": "str",
                "classCode": "str",
                "classType": "str",
                "location": "str",
                "grades": "str",
                "subjects": "str",
                "subjectCodes": "str",
                "course.sourcedId": "str",
                "school.sourcedId": "str",
                "student_classlink_source_id": "str",
            },  # Using pandas data types
        )
    ):

        

        file_chunk.replace(np.nan, None, inplace=True)

        # Rename columns to match the database table's column names

        file_chunk.rename(
            columns={
                "sourcedId": "SOURCEDID",
                "status": "STATUS",
                "title": "TITLE",
                "dateLastModified": "DATELASTMODIFIED",
                "classCode": "CLASSCODE",
                "classType": "CLASSTYPE",
                "location": "LOCATION",
                "grades": "GRADES",
                "subjects": "SUBJECTS",
                "subjectCodes": "SUBJECTCODES",
                "course.sourcedId": "COURSE_SOURCEDID",
                "school.sourcedId": "SCHOOL_SOURCEDID",
                "student_classlink_source_id": "STUDENT_CLASSLINK_SOURCE_ID",
            },
            inplace=True,
        )

        # Exclude columns 
        for col in [
            "terms",
            "resources",
            "course.href",
            "course.type",
            "school.href",
            "school.type"
        ]:
            if col in file_chunk.columns:
                file_chunk.drop(col, axis=1, inplace=True)

           

        try:
            insert_dataframe(
                table_name,
                file_chunk,
                schema=schema,
                if_exists=if_exists,
                chunksize=chunksize,
                dtype=db_dtype,  # This argument is for database column types during the insertion, ensure it's correctly set
                #method=method,
            )
            logging.info("Completed round.")
        except Exception as e:
            logging.error(f"Could not load data for round {idx} because: {e}")



@task
def truncate_staging_table():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_CLEVER_CLASSLINK_DATA_LOAD_PKG.pc_classlink_stage_trun", "x_load_Status")
    

@task
def run_classes_d_procedure():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_CLEVER_CLASSLINK_DATA_LOAD_PKG.pc_classlink_classes_d", "x_load_Status")


@task 
def run_classes_f_procedure():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_CLEVER_CLASSLINK_DATA_LOAD_PKG.pc_classlink_classes_f", "x_load_Status")

