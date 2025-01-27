import sys
import numpy as np
from prefect import task
from sqlalchemy import VARCHAR, Float
from src.env_config import *
from src.etl import *
sys.path.append(ROOT_DIR)
from scripts.oracledb_connection.internals.db import *

@task
def load_assessment_data_to_staging():
    folder_path = INCREMENTAL_DATA_FILE_PATH

    # Fetch the latest file with the given prefix
    latest_file = get_latest_file_from_folder(folder_path, file_prefix='assessment_results_incremental_')
    
    if latest_file is None:
        raise ValueError("No file found with today's date.")

    data_type = {
    "TEST_RESULT_BID": VARCHAR(100),
    "INSTRUCTIONAL_AREA_HIGH": Float(),
    "INSTRUCTIONAL_AREA_LOW": Float(),
    "INSTRUCTIONAL_AREA_SCORE": Float(),
    "INSTRUCTIONAL_STD_ERR": Float(),
    "NORMS_PERCENTILE": Float(),
    "QUANTILE_ORIGINAL": VARCHAR(100),
    "RESPONSE_DISENGAGED_PERCENTAGE": Float(),
    "STANDARD_ERROR": Float(),
    "ADMINISTRATION_END_DATE_TIME": VARCHAR(100),
    "ADMINISTRATION_START_DATE_TIME": VARCHAR(100),
    "GRADE": VARCHAR(100),
    "INSTRUCTIONAL_AREA_BID": VARCHAR(100),
    "INSTRUCTIONAL_AREA_NAME": VARCHAR(100),
    "INSTRUCTIONAL_AREA_SCORE": Float(),
    "INSTRUCTIONAL_AREA_STD_ERR": Float(),
    "INSTRUCTIONAL_AREA_LOW": Float(),
    "INSTRUCTIONAL_AREA_HIGH": Float(),
    "LEXILE_SCORE": VARCHAR(100), 
    "LEXILE_MAX": VARCHAR(100),
    "LEXILE_MIN": VARCHAR(100),
    "LEXILE_RANGE": VARCHAR(100),
    "MODIFIED_DATE_TIME": VARCHAR(100),
    "PARENT_SCHOOL_BID": VARCHAR(100),
    "QUANTILE_MAX": VARCHAR(100),
    "QUANTILE_MIN": VARCHAR(100),
    "QUANTILE_RANGE": VARCHAR(100),
    "QUANTILE_SCORE": VARCHAR(100),
    "SCHOOL_BID": VARCHAR(100),
    "STATUS": VARCHAR(100),
    "SUBJECT_AREA": VARCHAR(100),
    "ACCOMMODATIONS_NAME": VARCHAR(500), 
    "ACCOMMODATIONS_CATEGORY": VARCHAR(100), 
    "DURATION": Float(),  
    "GROWTH_EVENT_YN": VARCHAR(10),  
    "IMPACT_OF_DISENGAGEMENT": VARCHAR(100),  
    "ITEMS_CORRECT": Float(),  
    "ITEMS_SHOWN": Float(),  
    "ITEMS_TOTAL": Float(),  
    "RIT": Float(),  
    "RIT_SCORE_HIGH": Float(),  
    "RIT_SCORE_LOW": Float(),  
    "STUDENT_BID": VARCHAR(100),  
    "TERM_BID": VARCHAR(100),  
    "TEST_KEY": VARCHAR(100),  
    "TEST_NAME": VARCHAR(100),  
    "TEST_TYPE": VARCHAR(100),  
}

    # Load the data into the staging table
    lazy_load_df_into_db(
        file_path=latest_file,
        table_name="FEV_NWEA_ASSESSMENT_STAGE",
        chunksize=10000,
        if_exists="append",
        schema="DWH_STAGING",
        db_dtype=data_type,
        parse_dates=[
            "ADMINISTRATION_START_DATE_TIME",
            "ADMINISTRATION_END_DATE_TIME",
            "MODIFIED_DATE_TIME",
        ],
    )



def lazy_load_df_into_db(
    file_path: str,
    table_name: str,
    schema: str = None,
    chunksize: int = 10000,
    db_dtype: dict = None,
    if_exists="fail",
    parse_dates=None,
    #method="multi",
) -> None:
    """
    Lazily load in a dataframe and pass in the chunks to a dataframe
    """

    for idx, file_chunk in enumerate(
        pd.read_csv(
            file_path,
            chunksize=chunksize,
            parse_dates=parse_dates  # Automatic date parsing
        )
    ):  
    

        
        file_chunk.replace(np.nan, None, inplace=True)

        if "NORMS_TYPE" in file_chunk.columns:
            file_chunk.drop("NORMS_TYPE", axis=1, inplace=True)

        if "NORMS_REFERENCE" in file_chunk.columns:
            file_chunk.drop("NORMS_REFERENCE", axis=1, inplace=True)

        # convert to single timestamp format
        if "MODIFIED_DATE_TIME" in file_chunk.columns:
            file_chunk['MODIFIED_DATE_TIME'] = pd.to_datetime(file_chunk['MODIFIED_DATE_TIME'], 
                format = 'ISO8601', utc = True).dt.strftime('%d-%b-%y')    
        
        if "ADMINISTRATION_START_DATE_TIME" in file_chunk.columns:
            file_chunk['ADMINISTRATION_START_DATE_TIME'] = pd.to_datetime(file_chunk['ADMINISTRATION_START_DATE_TIME'], 
                format = 'ISO8601', utc = True).dt.strftime('%d-%b-%y')  
        
        if "ADMINISTRATION_END_DATE_TIME" in file_chunk.columns:
            file_chunk['ADMINISTRATION_END_DATE_TIME'] = pd.to_datetime(file_chunk['ADMINISTRATION_END_DATE_TIME'], 
                format = 'ISO8601', utc = True).dt.strftime('%d-%b-%y') 

        if idx % 10 == 0:
            logging.info(f"On round {idx}")

        try:
            insert_dataframe(
                table_name,
                file_chunk,
                schema=schema,
                if_exists=if_exists,
                chunksize=chunksize,
                dtype=db_dtype,
                #method=method,
            )
            logging.info("Completed round.")
        except Exception as e:
            logging.info(f"Could not load data for round {idx} because: {e}")


@task
def truncate_assessment_staging():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_NWEA_DIMENSION_DATA_LOAD_PKG.pc_trun_assessment_d","x_load_Status")

@task
def run_assessment_d_procedure():
        prepare_data_in_data_warehouse("DWH_DATA.FEV_NWEA_DIMENSION_DATA_LOAD_PKG.pc_nwea_assessment_d","x_load_Status")

@task
def run_assessment_f_procedure():
        prepare_data_in_data_warehouse("DWH_DATA.FEV_NWEA_DIMENSION_DATA_LOAD_PKG.pc_nwea_assessment_f","x_load_Status")