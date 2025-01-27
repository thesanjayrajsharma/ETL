import logging
import sys
import numpy as np
import pandas as pd
from datetime import datetime
sys.path.append('/home/fevdevadmin/etl_repo/ETL/scripts/')
from oracledb_connection.internals.db import *




def lazy_load_userpilot_adhoc__df_into_db(
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

    
    columns_to_read = [
        "User ID", "Name", "Web Sessions", "Score", "Feedback", 
         "Submitted At", 
        "Email", "Hostname", "School Id", 
        "School Name", "District Name", "Role Name"
    ]

    for idx, file_chunk in enumerate(
        pd.read_csv(
            file_path,
            chunksize=chunksize,
            usecols=columns_to_read,  # Only read the specified columns
            dtype={
                "User ID": "str",
                "Name": "str",
                "Web Sessions": "int",
                "Score": "float",
                "Feedback": "str",
                "Submitted At": "str",  # Could also parse as datetime if needed
                "Email": "str",
                "Hostname": "str",
                "School Id": "str",
                "School Name": "str",
                "District Name": "str",
                "Role Name": "str"
            }  # Using pandas data types
        )
    ):
        file_chunk.replace(np.nan, None, inplace=True)

        file_chunk.rename(columns={
                "User ID": "user_id",
                "Name": "name",
                "Web Sessions": "sessions",
                "Score": "score",
                "Feedback": "feedback",
                "Submitted At": "event_create_date",  # Could also parse as datetime if needed
                "Email": "email",
                "Hostname": "hostname",
                "School Id": "schoolid",
                "School Name": "school_name",
                "District Name": "district_name",
                "Role Name": "role_name"
            # Add other renaming if needed
        }, inplace=True
        )
        
        
        file_chunk['event_create_date'] = pd.to_datetime(file_chunk['event_create_date'],errors='coerce').dt.tz_localize(None)

        try:
            insert_dataframe(
                table_name,
                file_chunk,
                schema=schema,
                if_exists=if_exists,
                chunksize=chunksize,
                dtype=db_dtype,
            )
            logging.info(f"Completed chunk {idx}.")
        except Exception as e:
            logging.error(f"Could not load data for chunk {idx} because: {e}")

def userpilot_adhoc():
    try:  
        lazy_load_userpilot_adhoc__df_into_db(
            file_path='/home/fevdevadmin/etl_repo/ETL/scripts/adhoc/historical_data.csv',
            table_name="FEV_USERPILOT_STG",
            schema="DWH_STAGING",
            chunksize=10000,
            if_exists="append",
            db_dtype=None,
        )
        
    except Exception as e:
        (
            print("failed the process")
        )
        
        
userpilot_adhoc()      
        