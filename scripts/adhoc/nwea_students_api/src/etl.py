"""Functions to help with ETL process"""
from ast import parse
from datetime import datetime
import logging
import multiprocessing
import os
import shutil
import time
import pandas as pd
from prefect import task
import requests
from src.env_config import *
SLEEP_TIME = 600  # Sleep time in seconds




def generate_api_header(token_url: str = 'https://api.nwea.org/auth/v1/token',
                        grant_type='client_credentials'):
    """Get access token and generate api headers to use for API connections"""
    auth_code = NWEA_AUTH
    api_key = NWEA_API_KEY

    token_headers = {
        'Authorization': auth_code,
        'apikey': api_key,
        'grant_type': grant_type,
        'Content-Length': '0'
    }

    try:
        resp_json = requests.post(token_url, headers=token_headers).json()
        access_token = resp_json['access_token']
        api_headers = {
            'Content/Type': 'application/json',
            'apikey': api_key,
            'Authorization': f'Bearer {access_token}'
        }

        return api_headers
    except Exception as e:
        logging.info(f"Could not connect to API because: {e}")
        return None


def apply_eval(series_val):
    """Helper function to apply eval to a series value"""
    if type(series_val) in [str, bytes]:
        return eval(series_val)
    elif type(series_val) in [list, dict]:
        return series_val
    return None




def api_request(student_bid, api_headers):
    api_url = f'https://api.nwea.org/students/v2/{student_bid}/results'            
    response = requests.get(api_url, headers=api_headers)
    return response,student_bid
                      

def apply_eval(series_val):
    """Helper function to apply eval to a series value"""
    if type(series_val) in [str, bytes]:
        return eval(series_val)
    elif type(series_val) in [list, dict]:
        return series_val
    return None


def normalize_json_col(
    data: pd.DataFrame,
    json_col: str,
    new_col_names: list = None,
    col_mapping: dict = None,
    explode_col=False,
) -> pd.DataFrame:
    """Normalize a json column in a dataframe"""

    # touching the original data could disrupt later operations
    data_copy = data.copy(deep=True)

    # unnormalized columns are sometimes strings, so eval them
    # this step is slow, best to try and replace
    data_copy[json_col] = data_copy[json_col].apply(apply_eval)

    if explode_col:
        # must ignore index to match against original dataset
        exploded_data = data_copy[json_col].explode(ignore_index=False)
        exploded_index = exploded_data.index
        normalized_data = pd.json_normalize(exploded_data)
        to_drop=normalized_data.filter(regex='^items.').columns
        normalized_data.drop(columns=to_drop, inplace=True)
        normalized_data.columns = new_col_names
        normalized_data.index = exploded_index
        final_data = pd.merge(
            data_copy, normalized_data, left_index=True, right_index=True
        )

        # reset index to match original dataset
        final_data.reset_index(drop=True, inplace=True)
    else:
        data_copy["key"] = data_copy.index
        normalized_data = pd.json_normalize(data_copy[json_col])
        
        normalized_data["key"] = normalized_data.index
        final_data = normalized_data.merge(data_copy, on="key")
        final_data.drop("key", axis=1, inplace=True)

        # need to use map since final data has all columns
        final_data.rename(columns=col_mapping, inplace=True)

    final_data.drop(json_col, axis=1, inplace=True)

    return final_data



def normalize_instructional_areas(api_data):
    api_data_copy = api_data.copy(deep=True)
    failed_records = []  # To store failed records
    if "instructionalAreas" in api_data_copy.columns:
        rows = []
        # Iterate over each row
        for idx, row in api_data_copy.iterrows():
            try:
                instructional_areas = row['instructionalAreas']
                testResultBid = row['testResultBid']  # Fetch testResultBid from api_data
                # Iterate over each instructional area
                for area in instructional_areas:
                    row_data = {
                        'testResultBid': testResultBid,  # Add testResultBid to the row data
                        'INSTRUCTIONAL_AREA_BID': area.get('instructionalAreaBid'),
                        'INSTRUCTIONAL_AREA_NAME': area.get('instructionalAreaName'),
                        'INSTRUCTIONAL_AREA_STD_ERR': area.get('standardError'),
                        'INSTRUCTIONAL_AREA_SCORE': area.get('score'),
                        'INSTRUCTIONAL_AREA_HIGH': area.get('scoreHigh'),
                        'INSTRUCTIONAL_AREA_LOW': area.get('scoreLow')
                    }
                    rows.append(row_data)
            except Exception as e:
                # Handle the exception by storing failed records
                failed_records.append(row)
                failed_df=pd.DataFrame(failed_records)
                failed_df['INSTRUCTIONAL_AREA_BID']=None
                failed_df['INSTRUCTIONAL_AREA_NAME']=None
                failed_df['INSTRUCTIONAL_AREA_STD_ERR']=None
                failed_df['INSTRUCTIONAL_AREA_SCORE']=None
                failed_df['INSTRUCTIONAL_AREA_HIGH']=None
                failed_df['INSTRUCTIONAL_AREA_LOW']=None                      
        normalized_df = pd.DataFrame(rows)
        try:
            final_data = pd.merge(api_data_copy, normalized_df, on="testResultBid", how="inner")
            # Drop the instructionalAreas column
            final_data.drop("instructionalAreas", axis=1, inplace=True)
            return final_data
        except Exception as merge_e:
          
            return None  # Return None as the final data
        
    return api_data_copy  # Return the original data if "instructionalAreas" column is not present





def filter_columns(data: list, columns: list = ['testResultBid', 'studentBid', 'studentId', 'schoolBid', 
    'districtBid', 'term', 'subject', 'grade', 'testName', 'testKey', 
    'testType', 'testDate', 'administrationDate', 'administrationEndDate', 
    'duration', 'status', 'rit', 'standardError', 'ritScoreHigh', 'ritScoreLow', 
    'items', 'instructionalAreas', 'accommodations', 'norms', 'gradeCustom', 
    'responseDisengagedPercentage', 'impactOfDisengagement', 'timezone', 
    'growthEventYn', 'lexile']):
    
    """Filters json data based on required columns and saves bad data to CSV"""

    filtered_data = []
    for record in data:
        # Check norms subkeys
        if 'norms' in record:
            for norm in record['norms']:
                # Check if each subkey is present, if not, set it to None
                if 'type' not in norm:
                    norm['type'] = None
                if 'reference' not in norm:
                    norm['reference'] = None
                if 'percentile' not in norm:
                    norm['percentile'] = None
        
        # Handle missing keys by adding them with default values
        for col in columns:
            if col not in record:
                if col == 'items':
                    record[col] = {"shown": None, "correct": None, "total": None}
                elif col == 'instructionalAreas':
                    record[col] = []
                elif col == 'accommodations':
                    record[col] = []
                elif col == 'term':
                    record[col] = {"season": None, "iweek": None, "termBid": None, "termName": None, "termSeq": None}
                elif col == 'lexile':
                    record[col] = {"score": None, "range": None, "min": None, "max": None}
                elif col == 'norms':
                    record[col] = [{
                         "type": None,
                         "reference": None,
                         "percentile": None
                    }]
                else:
                    record[col] = None

        filtered_data.append(record)    
    return filtered_data



def handle_api_response(api_data):
    if len(api_data) > 0:  
        if "norms" in api_data.columns:
            api_data = normalize_json_col(
                api_data,
                json_col="norms",
                new_col_names=[
                    "NORMS_TYPE",
                    "NORMS_REFERENCE",
                    "NORMS_PERCENTILE",
                ],
                explode_col=True,
            )
        
        if "instructionalAreas" in api_data.columns and api_data["instructionalAreas"] is not None:
             api_data = normalize_instructional_areas(api_data)
      
    return api_data




def process_student_data(bid, api_headers):
    response,student_bid = api_request(bid, api_headers)
    
    if response and response.status_code == 200:
        response_data = response.json()
        filtered_data=filter_columns(response_data)
        normalized_data = pd.json_normalize(filtered_data)
        data_df = pd.DataFrame(normalized_data)
        data = handle_api_response(data_df)
        return {"student_bid": student_bid, 
                "data": data, 
                "status_code": response.status_code
                }
        
    elif response and response.status_code != 200:
        # API did not return a successful status code
        return {"student_bid": student_bid,
                "status_code": response.status_code, 
                "reason": f"API error: HTTP {response.status_code}"
                }
    elif response and response.status_code == 429:  # HTTP status code 429 indicates rate limit exhaustion
     # Sleep and retry after some time
        time.sleep(SLEEP_TIME)
        return process_student_data(bid, api_headers)  # Retry the API call
    
    else:
        # General error, including connection issues
        return {"student_bid": bid,
                "status_code": response.status_code,
                "reason": "No response or network issue"
                }



def chunked_student_list(student_list, chunk_size=50000):
    """Yield successive chunk_size chunks from student_list."""
    for i in range(0, len(student_list), chunk_size):
        yield student_list[i : i + chunk_size]

def run_student_assessment_pull(src=None):
    ''' Check if src is a string (assumed to be a CSV file) or a DataFrame'''
    if isinstance(src, str):
        student_list = pd.read_csv(src)["STUDENT_BID"].values.tolist()
    else:
        ''' Assume src is a DataFrame'''
        student_list = src["STUDENT_BID"].values.tolist()
    # Clear the temporary directory if it's the first call
    
    if os.path.exists(STUDENT_TEMP_PATH):
        shutil.rmtree(STUDENT_TEMP_PATH)
        os.makedirs(STUDENT_TEMP_PATH, exist_ok=False)

    api_headers = generate_api_header()

    if api_headers is None:
        raise ValueError("Could not generate API headers")

    batch_number = 0
  
        # Process each chunk of students
    for  idx,students_chunk in enumerate(
        chunked_student_list(student_list, chunk_size=5000)
    ):
        students_chunk = [
            student_bid
            for student_bid in students_chunk
        ]
        if len(students_chunk) == 0:
            logging.info(f"No new students to process, stepping over batch #{batch_number}")
            continue

        logging.info(f"Processing batch #{batch_number} with {len(students_chunk)} students")

        # Check if the API headers are still valid
        test_resp = requests.get(
            f"https://api.nwea.org/students/v2/{students_chunk[0]}/results", headers=api_headers
        )
        
        if test_resp.status_code == 401:
            logging.error("Got a status code 401, reconnecting to API")
            api_headers = generate_api_header()
        logging.info(f"Processing batch #{batch_number}")
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            results = pool.starmap(
                process_student_data,
                [
                    (student_bid, api_headers)
                    for student_bid in students_chunk
                ],
            )
        errors_batch = []
        empty_data_batch=[]
        student_data_batch = pd.DataFrame() 
        for result in results:
            if result["status_code"] != 200: 
                errors_batch.append({
                    "student-bid": result["student_bid"],
                    "error-type": result["status_code"],
                    "date": datetime.now(),
                })
            elif result['status_code']==200 and result['data'] is None:
                empty_data_batch.append({
                    "student-bid": result["student_bid"],
                    "error-type": 'no data',
                    "date": datetime.now(),
                })
            elif result['status_code']==200 and result['data'] is not None:
                data = result["data"]
                student_data_batch = pd.concat([student_data_batch, data], ignore_index=True)
                  
        logging.info(f"Found {len(student_data_batch)} new students in batch #{batch_number}")

        batch_number += 1
        temp_file_path = f"{STUDENT_TEMP_PATH}/batch_{batch_number}.csv"
        student_data_batch.to_csv(temp_file_path, index=False, header=True)
        #log erros after each batch
        
        if errors_batch:
            if not os.path.exists(STUDENT_ERRORS_PATH):
                os.makedirs(STUDENT_ERRORS_PATH, exist_ok=False)
            timestamp = datetime.now().strftime("%d-%m-%Y")
            errors_logs_file = f"{STUDENT_ERRORS_PATH}/student_errors_{timestamp}.csv"
            pd.DataFrame(errors_batch).to_csv(errors_logs_file, index=False, mode='a', header=not os.path.exists(errors_logs_file))

        #log erros after each batch
        
        if len(empty_data_batch)>0:
            if not os.path.exists(STUDENT_NODATA_PATH):
                os.makedirs(STUDENT_NODATA_PATH, exist_ok=False)
            timestamp = datetime.now().strftime("%d-%m-%Y")
            errors_logs_file = f"{STUDENT_NODATA_PATH}/no_data_{timestamp}.csv"
            empty_data_batch_df=pd.DataFrame(empty_data_batch)
            pd.DataFrame(empty_data_batch).to_csv(errors_logs_file, index=False, mode='a', header=not os.path.exists(errors_logs_file))

        time.sleep(300)
    
    #Concatenate all non-empty temporary CSVs
    nwea_student_records = pd.concat(
        [
            pd.read_csv(f"{STUDENT_TEMP_PATH}/{file}",dtype=str) 
            for file in os.listdir(STUDENT_TEMP_PATH)
            if os.path.getsize(f"{STUDENT_TEMP_PATH}/{file}") > 0  # Skip empty files
        ],
        ignore_index=True 
    )
    
    #save to the actual data file
    if not os.path.exists(STUDENT_DATA_PATH):
        os.makedirs(STUDENT_DATA_PATH)
    timestamp = datetime.now().strftime("%d-%m-%Y")
    nwea_student_records.to_csv(f"{STUDENT_DATA_PATH}/student_data_{timestamp}.csv", index=False)
    logging.info("All data appended to the actual data file after max retries.")

     
