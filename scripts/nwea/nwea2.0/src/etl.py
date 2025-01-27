"""Functions to help with ETL process"""
from ast import parse
import multiprocessing
import shutil
import time

from prefect import task
import requests
from tqdm import tqdm
from src.api import (
    pull_data_from_api,
    generate_api_header,
    connect_to_api,
)
from src.env_config import *
from src.config import data_cols_config, student_cols_config
from src.validation import *
import dask.dataframe as dd





def apply_eval(series_val):
    """Helper function to apply eval to a series value"""
    if type(series_val) in [str, bytes]:
        return eval(series_val)
    elif type(series_val) in [list, dict]:
        return series_val
    return None


def is_datetime_with_datetime_lib(value):
    """Check if value is a datetime object using datetime library"""
    try:
        datetime.strptime(value, "%Y-%m-%d")  # You can add more formats if needed
        return True
    except (ValueError, TypeError):
        try:
            parse(value)
            return True
        except (ValueError, TypeError):
            return False


def is_datetime_with_pandas(value):
    """Check if value is a datetime object using pandas library"""
    try:
        pd.to_datetime(value)
        return True
    except (ValueError, TypeError, pd._libs.tslibs.np_datetime.OutOfBoundsDatetime):
        return False


def is_datetime(value):
    """Check if value is a datetime object"""
    return is_datetime_with_datetime_lib(value) or is_datetime_with_pandas(value)


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


def get_bid_date_cutoffs(data_path: str, date_col="MODIFIED_DATE_TIME") -> pd.DataFrame:
    """Get date cutoffs for each bid in the data store"""
    data = pd.read_csv(data_path, parse_dates=[date_col])
    return data.groupby("PARENT_SCHOOL_BID")[[date_col]].max()


def filter_based_on_max_dates(
    result_set: list, max_date: datetime, date_col="modifiedDateTime"
) -> list:
    """Filter result set based off of max date currently in results data"""

    # format date so you can compare it
    max_date = pd.to_datetime(max_date).to_pydatetime()
    max_date = max_date.date()

    results = []
    for result in result_set:
        date_string = format_date_string(result[date_col])
        result[date_col] = convert_str_to_date(date_string)
        if result[date_col].date() > max_date:
            results.append(result)

    return results


def format_date_string(date_string: str) -> str:
    """Helper function to format dates into a standard '%Y-%m-%dT%H:%M:%SZ'
    format"""

    date_string = date_string.split(".")[0]

    # current formatting looks for a 'Z', so add it on
    if date_string[-1] != "Z":
        date_string += "Z"
    if "T" not in date_string:
        date_string = date_string.replace(" ", "T")

    return date_string


def convert_str_to_date(
    date_string: str, date_format: str = "%Y-%m-%dT%H:%M:%SZ"
) -> datetime:
    """Convert a date string to an actual datetime object for comparisons"""
    return datetime.strptime(date_string, date_format)


def get_max_date_from_results(result_set: list) -> datetime:
    """pull max date from the modifiedDateTime key in NWEA result set"""
    return max(result["modifiedDateTime"] for result in result_set)

def filter_columns(data: list, columns: list = [
    'testResultBid', 'studentBid', 'schoolBid', 'termBid', 'subjectArea', 
    'grade', 'testName', 'testKey', 'testType', 'growthEventYn', 'duration', 
    'status', 'rit', 'standardError', 'ritScoreHigh', 'ritScoreLow', 'items', 
    'quantile','lexile', 'responseDisengagedPercentage', 'impactOfDisengagement', 
    'administrationStartDateTime', 'administrationEndDateTime', 
    'modifiedDateTime','instructionalAreas', 'accommodations', 'norms'
]):
    
    """Filters json data based on required columns"""

    filtered_data = []
    for record in data:
        # Check norms subkeys
        if 'norms' in record:
            for norm in record['norms']:
                # Check if each subkey is present, if not, set it to None
                if 'percentile' not in norm:
                    norm['percentile'] = None
                if 'reference' not in norm:
                    norm['reference'] = None
                if 'type' not in norm:
                    norm['type'] = None
        
        # Handle missing keys by adding them with default values
        for col in columns:
            if col not in record:
                if col == 'items':
                    record[col] = {"shown": None, "correct": None, "total": None}
                elif col == 'accommodations':
                    record[col] = []
                elif col == 'lexile':
                    record[col] = {"score": None, "range": None, "min": None, "max": None}
                elif col == 'quantile':
                    record[col] = {"score": None, "maximum": None, "minimum": None,'range':None,'original':None}
                else:
                     record[col] = None

        filtered_data.append(record)    
    return filtered_data


@task
def run_assessment_data_pull(
   
    cutoffs_path: str = None,
    bids_path: str = SCHOOL_BIDS_PATH,
    api_url: str = "https://api.nwea.org/test-results/v1/growth",
) -> None:
    """Final function to connect to the API & pull the data"""
    
    # keep track of time elapsed in function
    start_time = time.time()
    api_headers = generate_api_header()

    if api_headers is None:
        raise ValueError(f"Could not generate API headers {api_headers}")

    # get initial data necessary for data run, if it exists
    # NOTE:  we were filtering records based off of the modifiedDateTime
    # column from the NWEA API, but there was concern that the data would
    # not be accurately filtered this way.  We are now 
    # ingesting the whole API response and filtering the data
    # during the ETL process inside the Oracle Database
    if cutoffs_path is not None:
        cutoffs = pd.read_csv(cutoffs_path, parse_dates=["MAX(MODIFIED_DATETIME)"])
        cutoffs = cutoffs.set_index("PARENT_BID")
    else:
        # if no cutoffs are provided, set to None
        # means no date filtering will be done
        max_date = None
        cutoffs = None

    # master list of bids
    bids = pd.read_csv(bids_path)
    bids = bids["schoolBid"].values.tolist()
    
    # if /temp directory exists, remove it
    if os.path.exists(TEMP_FILE_PATH):
        shutil.rmtree(TEMP_FILE_PATH)
        

    # create empty directory for temp files
    os.makedirs(TEMP_FILE_PATH, exist_ok=True)

    errors = []
    # pull data from api for each bid in the list
    for bid in tqdm(bids[0:]):
        logging.info(f"Pulling data for school with bid: {bid}")

        # check if bid exists in current data
        now = datetime.now()

        # check if max_date exists for bid
        if cutoffs is not None:
            bid_currently_exists = bid in cutoffs.index
            if bid_currently_exists:
                max_date = cutoffs.loc[bid].values[0]
            else:
                max_date = None
        api_params = {"school-bid": bid}

        # pull from the API
        api_resp = connect_to_api(
            api_url=api_url, api_headers=api_headers, api_params=api_params
        )

        if api_resp.status_code != 200 and api_resp.status_code != 401:
            errors.append(
                {
                    "school-bid": bid,
                    "error-type": "api-error",
                    "message": api_resp.status_code,
                    "date": now,
                }
            )
            continue

        elif api_resp.status_code == 401:
            api_headers = generate_api_header()
            api_resp = connect_to_api(
                api_url=api_url, api_headers=api_headers, api_params=api_params
            )

            if api_resp.status_code != 200:
                errors.append(
                    {
                        "school-bid": bid,
                        "error-type": "api-error",
                        "message": api_resp.status_code,
                        "date": now,
                    }
                )
                continue

        api_data, error_dict = pull_data_from_api(
            api_resp,
            api_url=api_url,
            api_headers=api_headers,
            api_params=api_params,
            max_date=max_date,
        )
        if len(error_dict) > 0:
            # some functions don't return a school-bid, so add it here
            if error_dict.get("school-bid") == None:
                error_dict["bid"] = bid
                error_dict["date"] = now
            errors.append(error_dict)


        if len(api_data) > 0:
            
            filtered_api_data=filter_columns(api_data)
            api_data = pd.DataFrame(filtered_api_data)
            logging.info(f"Found {api_data.shape[0]} new rows for bid: {bid}")

            # if set to True, will abort this session
            has_formatting_errors = False

            # normalize json columns
            try:
                if "items" in api_data.columns:
                    api_data = normalize_json_col(
                        api_data,
                        json_col="items",
                        new_col_names=None,
                        # this argument is necessary for non-exploded columns
                        col_mapping={
                            "shown": "ITEMS_SHOWN",
                            "correct": "ITEMS_CORRECT",
                            "total": "ITEMS_TOTAL",
                        },
                        explode_col=False,
                    )
                else:
                    api_data["ITEMS_SHOWN"] = None
                    api_data["ITEMS_CORRECT"] = None
                    api_data["ITEMS_TOTAL"] = None
            except Exception as e:
                logging.error(f"Error normalizing json for column 'items': {e}")
                error_dict = {
                    "school-bid": bid,
                    "error-type": "json-normalization-error",
                    "message": e,
                    "date": now,
                }
                errors.append(error_dict)
                has_formatting_errors = True

            try:
                if "norms" in api_data.columns:
                    api_data = normalize_json_col(
                        api_data,
                        json_col="norms",
                        new_col_names=[
                            "NORMS_PERCENTILE",
                            "NORMS_REFERENCE",
                            "NORMS_TYPE",
                        ],
                        explode_col=True,
                    )
                else:
                    api_data["NORMS_PERCENTILE"] = None
                    api_data["NORMS_REFERENCE"] = None
                    api_data["NORMS_TYPE"] = None

                # don't need this column
                # might be a faster way to do this
                api_data.drop(["NORMS_REFERENCE", "NORMS_TYPE"], axis=1, inplace=True)
            except Exception as e:
                logging.error(f"Error normalizing json for column 'norms': {e}")
                error_dict = {
                    "school-bid": bid,
                    "error-type": "json-normalization-error",
                    "message": e,
                    "date": now,
                }
                errors.append(error_dict)
                has_formatting_errors = True

            try:
                if "quantile" in api_data.columns:
                    api_data = normalize_json_col(
                        api_data,
                        json_col="quantile",
                        new_col_names=None,
                        col_mapping={
                            "score": "QUANTILE_SCORE",
                            "maximum": "QUANTILE_MAX",
                            "minimum": "QUANTILE_MIN",
                            "range": "QUANTILE_RANGE",
                            "original": "QUANTILE_ORIGINAL",
                        },
                        explode_col=False,
                    )
                else:
                    api_data["QUANTILE_SCORE"] = None
                    api_data["QUANTILE_MAX"] = None
                    api_data["QUANTILE_MIN"] = None
                    api_data["QUANTILE_RANGE"] = None
                    api_data["QUANTILE_ORIGINAL"] = None
            except Exception as e:
                logging.error(f"Error normalizing json for column 'quantile': {e}")
                error_dict = {
                    "school-bid": bid,
                    "error-type": "json-normalization-error",
                    "message": e,
                    "date": now,
                }
                errors.append(error_dict)
                has_formatting_errors = True

            try:
                if "instructionalAreas" in api_data.columns and api_data['instructionalAreas'].notna().all():
                    api_data = normalize_json_col(
                        api_data,
                        json_col="instructionalAreas",
                        new_col_names=[
                            "INSTRUCTIONAL_AREA_BID",
                            "INSTRUCTIONAL_AREA_NAME",
                            "INSTRUCTIONAL_AREA_SCORE",
                            "INSTRUCTIONAL_AREA_STD_ERR",
                            "INSTRUCTIONAL_AREA_LOW",
                            "INSTRUCTIONAL_AREA_HIGH",
                        ],
                        explode_col=True,
                    )
                else:
                    api_data["INSTRUCTIONAL_AREA_BID"] = None
                    api_data["INSTRUCTIONAL_AREA_NAME"] = None
                    api_data["INSTRUCTIONAL_AREA_SCORE"] = None
                    api_data["INSTRUCTIONAL_AREA_STD_ERR"] = None
                    api_data["INSTRUCTIONAL_AREA_LOW"] = None
                    api_data["INSTRUCTIONAL_AREA_HIGH"] = None
                    api_data.drop('instructionalAreas', axis=1, inplace=True)
            
            except Exception as e:
                logging.error(
                    f"Error normalizing json for column 'instructionalAreas': {e}"
                )
                error_dict = {
                    "school-bid": bid,
                    "error-type": "json-normalization-error",
                    "message": e,
                    "date": now,
                }
                errors.append(error_dict)
                has_formatting_errors = True

            try:
                if "lexile" in api_data.columns:
                    api_data = normalize_json_col(
                        api_data,
                        json_col="lexile",
                        new_col_names=None,
                        col_mapping={
                            "score": "LEXILE_SCORE",
                            "min": "LEXILE_MIN",
                            "max": "LEXILE_MAX",
                            "range": "LEXILE_RANGE",
                        },
                        explode_col=False,
                    )
                else:
                    api_data["LEXILE_SCORE"] = None
                    api_data["LEXILE_MIN"] = None
                    api_data["LEXILE_MAX"] = None
                    api_data["LEXILE_RANGE"] = None
            except Exception as e:
                logging.error(f"Error normalizing json for column 'lexile': {e}")
                error_dict = {
                    "school-bid": bid,
                    "error-type": "json-normalization-error",
                    "message": e,
                    "date": now,
                }
                errors.append(error_dict)
                has_formatting_errors = True

            api_data.drop_duplicates(
                subset=["testResultBid", "modifiedDateTime", "INSTRUCTIONAL_AREA_BID"],
                keep="last",
                inplace=True,
            )

            # useful for record keeping
            api_data["parentBid"] = bid

            # rename columns to match database
            api_data = api_data.rename(columns=data_cols_config)

            # detect if there were any errors in the formatting
            if len(api_data.columns) != len(data_cols_config):
                logging.error(f"Incorrect number of columns for bid: {bid}")
                error_dict = {
                    "school-bid": bid,
                    "error-type": "formatting-error",
                    "message": "Incorrect number of columns",
                    "date": now,
                }
                errors.append(error_dict)
                has_formatting_errors = True

            if sorted(api_data.columns.tolist()) != sorted(
                list(data_cols_config.values())
            ):  
                logging.error(f"Column names not correct for bid: {bid}")
                error_dict = {
                    "school-bid": bid,
                    "error-type": "formatting-error",
                    "message": "Incorrect column names",
                    "date": now,
                }
                errors.append(error_dict)
                has_formatting_errors = True

            if has_formatting_errors:
                continue

            # make sure column values are in the correct order
            # sidesteps data coming from API in an inconsistent way
            sorted_cols = sorted(api_data.columns.tolist())
            api_data = api_data[sorted_cols]

            # export data to csv file
            api_data.to_csv(f"{TEMP_FILE_PATH}/{bid}.csv", index=False)

    # concatenate all files in temp system into one file
    nwea_records = pd.concat(
        [pd.read_csv(f"{TEMP_FILE_PATH}/{file}") for file in os.listdir(TEMP_FILE_PATH)]
    )

    logging.info(f"num records to be exported: {nwea_records.shape[0]}")

    timestamp = datetime.now().strftime("%d-%m-%Y")

    if not  os.path.exists(BULK_DATA_FILE_PATH):
        os.makedirs(BULK_DATA_FILE_PATH)

 
    # export final version of data
    nwea_records.to_csv(f"{BULK_DATA_FILE_PATH}/assessment_results_{timestamp}.csv", index=False)

    if not  os.path.exists(ERRORS_FILE_PATH):
        os.makedirs(ERRORS_FILE_PATH)

    # export errors
    if len(errors)>0:
        pd.DataFrame(errors).to_csv(f"{ERRORS_FILE_PATH}/final_errors_{timestamp}.csv", index=False)

    end_time = time.time()
    meta_info = {
        "date": datetime.now().strftime("%d-%m-%Y"),
        "records_added": nwea_records.shape[0],
        "errors_added": len(errors),
        "time_elapsed": end_time - start_time,
    }

    logging.info(f"Data pull complete. {nwea_records.shape[0]} records added")
    logging.info(f"Time elapsed: {end_time - start_time} seconds")

    # export meta data to csv file
    meta_data = pd.DataFrame([meta_info])
    if not  os.path.exists(META_DATA_FILE_PATH):
        os.makedirs(META_DATA_FILE_PATH)
    meta_data.to_csv(f"{META_DATA_FILE_PATH}/meta_data_{timestamp}.csv", index=False)
    logging.info("Meta data exported to csv file, process completed")


 
'''get the last pull data'''

def get_latest_file_from_folder(folder_path, file_prefix="", file_extension=".csv", date_format="%d-%m-%Y"):
    # Get all files in the folder
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    
    # Filter files by prefix (if provided) and extension
    if file_prefix:
        files = [f for f in files if f.startswith(file_prefix)]
    files = [f for f in files if f.endswith(file_extension)]
   
    # Get today's date and format it
    today = datetime.today().strftime(date_format)
    today_file = f"{file_prefix}{today}{file_extension}"
    
    # Check if today's file exists in the folder
    if today_file not in files:
        raise(f'No file found with current date matching: {today_file}')
    try:
        latest_file = max(files, key=lambda x: datetime.strptime(x.rsplit("_", 1)[-1].replace(file_extension, ''), date_format))
    except ValueError as e:
        logging.error(f"Error parsing date from filename: {e}")
        return None

    latest_file_path = os.path.join(folder_path, latest_file)

    return latest_file_path




'''gets the last pull data'''



def get_second_latest_file_from_folder(folder_path, file_prefix, date_format="%d-%m-%Y"):
    """
    Returns the second latest file from a folder, filtering by a specific file prefix and using date parsing from filenames.

    Args:
        folder_path (str): The path of the folder containing files.
        file_prefix (str): The prefix used to filter files in the folder.
        date_format (str): The date format in the filenames, default is "%d-%m-%Y".
    
    Returns:
        str or None: Full path of the second latest file, or None if no file or only one file exists.
    """
    # Get all files in the folder that match the prefix
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f)) and f.startswith(file_prefix)]
    
    if len(files) <= 1:
        # No second file, possibly the first run
        return None

    # Parse dates from filenames and get the latest one
    try:
        latest_file = max(files, key=lambda x: datetime.strptime(x.rsplit("_", 1)[-1].replace('.csv', ''), date_format))
    except ValueError as e:
        logging.error(f"Error parsing date from filename: {e}")
        return None

    # Remove the latest file to get the second latest
    files.remove(latest_file)

    try:
        second_latest_file = max(files, key=lambda x: datetime.strptime(x.rsplit("_", 1)[-1].replace('.csv', ''), date_format))
    except ValueError as e:
        logging.error(f"Error parsing date from filename: {e}")
        return None
    # Construct the full path to the second latest file
    second_latest_file_path = os.path.join(folder_path, second_latest_file)

    return second_latest_file_path


def get_incremental_data():
    folder_path = BULK_DATA_FILE_PATH
    new_data_folder = INCREMENTAL_DATA_FILE_PATH

    if not os.path.exists(new_data_folder):
        os.makedirs(new_data_folder)

    timestamp = datetime.now().strftime("%d-%m-%Y")
    new_data_file = os.path.join(new_data_folder, f"assessment_results_incremental_{timestamp}.csv")

    # Get the latest file with Dask
    df_new_data = dd.read_csv(
        get_latest_file_from_folder(folder_path, file_prefix="assessment_results_"), 
        dtype=dtype_config, 
        assume_missing=True
    )

    # Get the second latest file (for last pulled data)
    last_pulled_file = get_second_latest_file_from_folder(folder_path, file_prefix='assessment_results_')
    
    if last_pulled_file is None:
        logging.info("First run, treating all data as new.")
        df_new_data = df_new_data.drop_duplicates()  # Drop duplicates using Dask
        df_new_data.to_csv(new_data_file, index=False, single_file=True)  
        return df_new_data.compute()  # Compute and return as pandas dataframe

    # Read last pulled data (using Dask as well)
    last_pulled_data = dd.read_csv(
        last_pulled_file, 
        dtype=dtype_config, 
        assume_missing=True
    )
    
    # Perform the comparison: filtering out already existing records
    last_pulled_data_set = last_pulled_data['TEST_RESULT_BID'].compute().unique()  # Get unique values from 'TEST_RESULT_BID'
    
    new_records = df_new_data[~df_new_data['TEST_RESULT_BID'].isin(last_pulled_data_set)]

    new_records = new_records.drop_duplicates()

    new_records.to_csv(new_data_file, index=False, single_file=True)  

    return new_records.compute()

    


@task
def get_student_bids():
    assessment_data = get_incremental_data()
    
    if assessment_data.empty:
        logging.error("No new assessment data available.")
        return pd.DataFrame()  # Return an empty DataFrame if no data
       
    # Collect STUDENT_BIDs to get student data
    student_bids = assessment_data[['STUDENT_BID']].drop_duplicates()
    
    timestamp = datetime.now().strftime("%d-%m-%Y")

    # Ensure STUDENT_BIDS_PATH exists before saving
    if not os.path.exists(STUDENT_BIDS_PATH):
        os.makedirs(STUDENT_BIDS_PATH)

    student_bids.to_csv(os.path.join(STUDENT_BIDS_PATH, f"student_bids_{timestamp}.csv"), index=False)
    
    return student_bids




def api_request(student_bid, api_headers):
    """Function to handle API request for a single student."""
    api_url = f"https://api.nwea.org/students/v2/{student_bid}"
    api_resp = requests.get(api_url, headers=api_headers)
    return api_resp, student_bid


def process_student_data(student_bid, existing_students, api_headers):
    """Worker function to process each student."""
    if student_bid not in existing_students:
        api_resp, student_bid = api_request(student_bid, api_headers)
        # Return the necessary data or error information
        return {
            "student_bid": student_bid,
            "data": api_resp.json() if api_resp.status_code == 200 else None,
            "error": api_resp.status_code,
        }


def chunked_student_list(student_list, chunk_size=50000):
    """Yield successive chunk_size chunks from student_list."""
    for i in range(0, len(student_list), chunk_size):
        yield student_list[i : i + chunk_size]


'''run_student_data_pull() function can either take a csv or df as input list.
   Ideally,
   - Csv as argument, to run the student data pull separately
   - df as argument, when the function runs as a part of assessment ETL
'''
@task
def run_student_data_pull(src=None, current_list=None, retry_count=0, max_retries=3):
    ''' Check if src is a string (assumed to be a CSV file) or a DataFrame'''
    if isinstance(src, str):
        student_list = pd.read_csv(src)["STUDENT_BID"].values.tolist()
    else:
        ''' Assume src is a DataFrame'''
        student_list = src["STUDENT_BID"].values.tolist()

    ''' Only load current list if it is present'''
    if current_list is not None:
        existing_students = pd.read_csv(current_list)["STUDENT_BID"].values.tolist()
    else:
        existing_students = []
        logging.info("No current student list provided, proceeding without filtering existing students.")

    # Clear the temporary directory if it's the first call
    if retry_count == 0:
        if os.path.exists(STUDENT_TEMP_PATH):
            shutil.rmtree(STUDENT_TEMP_PATH)

        # Create empty directory for temp files
        os.makedirs(STUDENT_TEMP_PATH, exist_ok=False)

    api_headers = generate_api_header()

    if api_headers is None:
        raise ValueError("Could not generate API headers")

    batch_number = 0
    '''list of the required fields to get from the API'''
    required_fields = [
        "dateOfBirth", "districtBid", "ethnicity", "ethnicityCustom",
        "firstName", "gender", "grade", "gradeCustom",
        "lastName", "middleName", "schoolBid", "stateStudentId",
        "studentBid", "studentId"
    ]
    
    cumulated_empty_data_ids=[]
    # Process each chunk of students
    for  idx,students_chunk in enumerate(
        chunked_student_list(student_list, chunk_size=5000)
    ):
        # Filter out students that already exist
        students_chunk = [
            student_bid
            for student_bid in students_chunk
            if student_bid not in existing_students
        ]
        if len(students_chunk) == 0:
            logging.info(f"No new students to process, stepping over batch #{batch_number}")
            continue

        logging.info(f"Processing batch #{batch_number} with {len(students_chunk)} students")

        # Check if the API headers are still valid
        test_resp = requests.get(
            f"https://api.nwea.org/students/v2/{students_chunk[0]}", headers=api_headers
        )
        api_headers = generate_api_header()
        if test_resp.status_code == 401:
            logging.error("Got a status code 401, reconnecting to API")
            api_headers = generate_api_header()
        logging.info(f"Processing batch #{batch_number}")
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            results = pool.starmap(
                process_student_data,
                [
                    (student_bid, existing_students, api_headers)
                    for student_bid in students_chunk
                ],
            )
        student_data_batch, errors_batch, empty_data_batch = [], [], []

        for result in results:
            '''collect the resposnes which give empty data ,the ids will be reprocessed again '''
            if result['error'] == 200 and result["data"] == '' and not isinstance(result["data"], dict):
                empty_data_batch.append({
                    "STUDENT_BID": result["student_bid"],
                    "error-type": "api-error",
                    "message": result['error'],
                    "date": datetime.now(),
                })
            elif result["error"] != 200: 
                errors_batch.append({
                    "student-bid": result["student_bid"],
                    "error-type": "api-error",
                    "message": result['error'],
                    "date": datetime.now(),
                })
            else:
                data = result["data"]
                if isinstance(data, dict):
                    '''Retain only the required fields, adding empty strings if they are missing'''
                    structured_data = {field: str(data.get(field, "")) for field in required_fields}
                    student_data_batch.append(structured_data)
                

        cumulated_empty_data_ids.extend([entry['STUDENT_BID'] for entry in empty_data_batch])
        

        batch_number += 1
        student_data_df = pd.DataFrame(student_data_batch).rename(columns=student_cols_config)     
        temp_file_path = f"{STUDENT_TEMP_PATH}/batch_{batch_number}_retry_{retry_count}.csv"
        student_data_df.to_csv(temp_file_path, index=False, header=True)
        logging.info(f"Adding {student_data_df.shape[0]} students to the temporary files.")
     
        #log erros after each batch
        if errors_batch:
            timestamp = datetime.now().strftime("%d-%m-%Y")
            errors_logs_file = f"{STUDENT_ERRORS_PATH}/student_errors_{timestamp}.csv"
            pd.DataFrame(errors_batch).to_csv(errors_logs_file, index=False, mode='a', header=not os.path.exists(errors_logs_file))

        logging.info(f"Finished batch #{batch_number}, waiting 00 minutes")
        time.sleep(600)

    # Retry if needed
    if len(cumulated_empty_data_ids) > 0 and retry_count < max_retries:    
        logging.info(f"Retrying with empty data for batch #{batch_number} (attempt {retry_count + 1})")
        retry_ids=pd.DataFrame(cumulated_empty_data_ids, columns=["STUDENT_BID"])
        run_student_data_pull(src=retry_ids, current_list=current_list, retry_count=retry_count + 1,max_retries=max_retries)
       
    
    #Concatenate all non-empty temporary CSVs
    nwea_student_records = pd.concat(
        [
            pd.read_csv(f"{STUDENT_TEMP_PATH}/{file}") 
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
    
    
    
