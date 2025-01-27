"""Functions to help with ETL process"""
import os
import time
import logging
import requests

import pandas as pd
import numpy as np

from datetime import datetime
from dateutil.parser import parse
from tqdm import tqdm
import multiprocessing

from src.api import (
    pull_data_from_api,
    generate_api_header,
    connect_to_api,
    pull_api_data_from_bid_list,
)
from src.config import data_cols_config, student_cols_config
from src.validation import validate_data, dtype_config


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


def run_data_pull(
    records_path: str = "data/parent_bids.csv",
    bids_path: str = "data/nwea.csv",
    errors_path: str = "db/errors.csv",
    api_url: str = "https://api.nwea.org/test-results/v1/growth",
) -> None:
    """Final function to connect to the API & pull the data"""

    # keep track of time elapsed in function
    start_time = time.time()

    api_headers = generate_api_header()

    if api_headers is None:
        raise ValueError("Could not generate API headers")

    # get initial data necessary for data run
    cutoffs = pd.read_csv(records_path, parse_dates=["MAX(MODIFIED_DATETIME)"])
    cutoffs = cutoffs.set_index("PARENT_BID")

    # master list of bids
    bids = pd.read_csv(bids_path)
    bids = bids["schoolBid"].values.tolist()

    # if data/temp directory exists, remove it
    if os.path.exists("data/temp"):
        for file in os.listdir("data/temp"):
            os.remove(f"data/temp/{file}")
        os.rmdir("data/temp")

    # create empty directory for temp files
    os.makedirs("data/temp", exist_ok=True)

    errors = []

    # pull data from api for each bid in the list
    for bid in tqdm(bids):
        logging.info(f"Pulling data for school with bid: {bid}")

        # check if bid exists in current data
        now = datetime.now()

        bid_currently_exists = bid in cutoffs.index
        if bid_currently_exists:
            max_date = cutoffs.loc[bid]
        else:
            max_date = None
        api_params = {"school-bid": bid}

        print(f"value of max date: {max_date}")

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
            max_date=max_date.values[0] if bid_currently_exists else None,
        )

        
        if len(error_dict) > 0:
            # some functions don't return a school-bid, so add it here
            if error_dict.get("school-bid") == None:
                error_dict["bid"] = bid
                error_dict["date"] = now
            errors.append(error_dict)

        if len(api_data) > 0:
            api_data = pd.DataFrame(api_data)
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
                if "instructionalAreas" in api_data.columns:
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

            try:
                validate_data(api_data, dtype_config, max_date)
            except Exception as e:
                logging.error(f"Error validating data: {e}")
                error_dict = {
                    "school-bid": bid,
                    "error-type": "validation-error",
                    "message": e,
                    "date": now,
                }
                errors.append(error_dict)

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
            api_data.to_csv(f"data/temp/{bid}.csv", index=False)

    # concatenate all files in temp system into one file
    nwea_records = pd.concat(
        [pd.read_csv(f"data/temp/{file}") for file in os.listdir("data/temp")]
    )

    print(f"num records to be exported: {nwea_records.shape[0]}")

    # export final version of data
    nwea_records.to_csv("db/nwea2.csv", index=False)

    # export errors
    pd.DataFrame(errors).to_csv("db/errors.csv", index=False)

    end_time = time.time()
    meta_info = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "records_added": nwea_records.shape[0],
        "errors_added": len(errors),
        "time_elapsed": end_time - start_time,
    }

    logging.info(f"Data pull complete. {nwea_records.shape[0]} records added")
    logging.info(f"Time elapsed: {end_time - start_time} seconds")

    # export meta data to csv file
    meta_data = pd.DataFrame([meta_info])
    current_meta_data = pd.read_csv("db/meta.csv")
    current_meta_data = pd.concat([current_meta_data, meta_data])
    current_meta_data.to_csv("db/meta.csv", index=False)
    logging.info("Meta data exported to csv file, process completed")


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


def run_student_data_pull(
    src="data/current_students.csv", current_list="db/students.csv"
):
    student_list = pd.read_csv(src)["STUDENT_BID"].values.tolist()
    existing_students = pd.read_csv(current_list)["STUDENT_BID"].values.tolist()

    os.makedirs("data/student_temp", exist_ok=False)
    api_headers = generate_api_header()

    if api_headers is None:
        raise ValueError("Could not generate API headers")

    batch_number = 0

    # Process each chunk of students
    for idx, students_chunk in enumerate(
        chunked_student_list(student_list, chunk_size=10000)
    ):
        # filter out students that already exist
        students_chunk = [
            student_bid
            for student_bid in students_chunk
            if student_bid not in existing_students
        ]

        if len(students_chunk) == 0:
            print(f"No new students to process, stepping over batch #{batch_number}")
            continue

        print(f"Processing batch #{batch_number} with {len(students_chunk)} students")

        # check if the api headers are still valid
        test_resp = requests.get(
            f"https://api.nwea.org/students/v2/{students_chunk[0]}", headers=api_headers
        )
        
        if test_resp.status_code == 401:
            print(f"Got a status code 401, reconnecing to API")
            api_headers = generate_api_header()

        print(f"Processing batch #{batch_number}")
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            results = pool.starmap(
                process_student_data,
                [
                    (student_bid, existing_students, api_headers)
                    for student_bid in students_chunk
                ],
            )

        # Handle results for this chunk
        student_data_batch = [result["data"] for result in results if result["data"]]
        print(f"Found {len(student_data_batch)} new students in batch #{batch_number}")

        errors_batch = [
            {
                "student-bid": result["student_bid"],
                "error-type": "api-error",
                "message": result["error"],
                "date": datetime.now(),
            }
            for result in results
            if not result["data"]
        ]

        batch_number += 1

        student_data_df = pd.DataFrame(student_data_batch)

        if "personalNeedsProfile" in student_data_df.columns:
            student_data_df.drop("personalNeedsProfile", axis=1, inplace=True)

        if "englishLearner" in student_data_df.columns:
            student_data_df.drop("englishLearner", axis=1, inplace=True)

        if "individualizedPrograms" in student_data_df.columns:
            student_data_df.drop("individualizedPrograms", axis=1, inplace=True)

        # rename columns to match database
        student_data_df = student_data_df.rename(columns=student_cols_config)

        # student_data_df = student_data_df.drop_duplicates()
        print(f"Adding {student_data_df.shape[0]} students to the database")
        student_data_df.to_csv(f"data/student_temp/{batch_number}.csv", index=False)

        # export errors
        student_errors_df = pd.DataFrame(errors_batch).drop_duplicates()
        student_errors_df.to_csv(
            f"data/student_temp/errors_{batch_number}.csv", index=False
        )

        # wait 20 minutes between batches
        print(f"Finished batch #{batch_number}, waiting 10 minutes")
        time.sleep(600)
