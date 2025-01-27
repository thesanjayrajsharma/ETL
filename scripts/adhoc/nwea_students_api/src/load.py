import os
import numpy as np
import pandas as pd
from datetime import datetime
import pandas as pd
import sys 
from src.env_config import *
sys.path.append(ROOT_DIR)
from scripts.oracledb_connection.internals.db import *
from datetime import datetime     
from prefect import flow, task
from src.config import col_rename,data_type


def lazy_load_df_into_db(
    file_path: str,
    table_name: str,
    schema: str = None,
    chunksize: int = 5000,
    db_dtype: dict = None,
    if_exists="fail",
    dtype: dict = None,
    parse_dates=None,
) -> None:
    """
    Lazily load in a dataframe and pass in the chunks to a dataframe
    """

    for idx, file_chunk in enumerate(
        pd.read_csv(
            file_path,
            chunksize=chunksize,
            usecols=['testResultBid', 'studentBid', 'schoolBid', 'subject', 'grade', 'testName', 'testKey',
                     'testType', 'administrationDate', 'administrationEndDate', 'duration', 'status', 'rit',
                     'standardError', 'ritScoreHigh', 'ritScoreLow', 'responseDisengagedPercentage',
                     'impactOfDisengagement', 'term.termBid','growthEventYn', 'lexile.score', 'lexile.range', 'lexile.min',
                     'lexile.max', 'items.shown', 'items.correct', 'items.total', 'accommodations',
                     'INSTRUCTIONAL_AREA_STD_ERR', 'INSTRUCTIONAL_AREA_SCORE',
                     'INSTRUCTIONAL_AREA_BID', 'INSTRUCTIONAL_AREA_NAME',
                     'INSTRUCTIONAL_AREA_HIGH', 'INSTRUCTIONAL_AREA_LOW', 'NORMS_PERCENTILE'],
            dtype=dtype,
            parse_dates=parse_dates  # Automatic date parsing
        )
    ):  
        # if idx not in {}:
        #     print(f"Skipping chunk {idx}")
        #     continue 

        invalid_records = []
        for date_column in ["administrationDate", "administrationEndDate"]:
            if date_column in file_chunk.columns:
                try:
                    pd.to_datetime(file_chunk[date_column], format="ISO8601", errors="raise")    
                except ValueError:
                    invalid_records.extend(file_chunk[file_chunk[date_column].apply(lambda x: pd.to_datetime(x, errors='coerce') is pd.NaT)].index)

           # Convert dates if they are convertible
        try:
            if "administrationDate" in file_chunk.columns:
                file_chunk['administrationDate'] = pd.to_datetime(file_chunk['administrationDate'], 
                                                                    format = 'ISO8601', utc = True).dt.strftime('%d-%b-%y')  

            if "administrationEndDate" in file_chunk.columns:
                file_chunk['administrationEndDate'] = pd.to_datetime(file_chunk['administrationEndDate'], 
                                                                    format = 'ISO8601', utc = True).dt.strftime('%d-%b-%y') 

        except Exception as e:
                raise            
            
        # Rename columns to match the database table's column names
        file_chunk.rename(columns=col_rename, inplace=True)


        # Exclude 'url' and 'uri' columns if present
        for col in ['studentId', 'districtBid','testDate','testDate','gradeCustom','timezone','term.season','term.iweek',
        'term.termName','items_instructionalAreas','type','reference','items_instructionalAreas','term.termSeq',
        ]:
            if col in file_chunk.columns:
                file_chunk.drop(col, axis=1, inplace=True)


        file_chunk.replace(np.nan, None, inplace=True)

        if "NORMS_TYPE" in file_chunk.columns:
            file_chunk.drop("NORMS_TYPE", axis=1, inplace=True)

        if "NORMS_REFERENCE" in file_chunk.columns:
            file_chunk.drop("NORMS_REFERENCE", axis=1, inplace=True)
        
        
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
        except Exception as e:
            raise
            

    '''iterate the files in temp folder and load to the db'''
def load_assessment_data_to_db(
    folder_path: str,
    table_name: str,
    schema: str = None,
    chunksize: int = 5000,
    if_exists="fail",
    dtype: dict = data_type,
    parse_dates=None,
) -> None:
    """
    Load all CSV files from a folder into the database table.
    """
    for file_name in os.listdir(folder_path):
        # Process only CSV files
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            lazy_load_df_into_db(
                file_path=file_path,
                table_name=table_name,
                schema=schema,
                chunksize=chunksize,
                db_dtype=data_type,
                if_exists=if_exists,
                     )

def truncate_assessment_staging():
    prepare_data_in_data_warehouse("DWH_DATA.FEV_NWEA_DIMENSION_DATA_LOAD_PKG.pc_trun_assessment_d","x_load_Status")

'''update procedure name'''

def run_assessment_adhoc_procedure():
        prepare_data_in_data_warehouse("DWH_DATA.FEV_NWEA_DIMENSION_DATA_LOAD_PKG.PC_nwea_assessment_adhoc","x_load_Status")

