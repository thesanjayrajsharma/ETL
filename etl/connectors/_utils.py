"""
Helper functions for connecting to and pulling data from the NWEA API.
"""

from datetime import datetime
from dateutil.parser import parse

import pandas as pd


def filter_based_on_max_dates(
    result_set: list, max_date: datetime, date_col="modifiedDateTime"
) -> list:
    """Filter result set based off of max date currently in results data"""

    results = []
    for result in result_set:
        if type(result[date_col]) is str:
            date_string = format_date_string(result[date_col])
            result[date_col] = convert_str_to_date(date_string)
        if result[date_col] > max_date:
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


def get_max_date_from_results(
    result_set: list, filter_col: str = "modifiedDateTime"
) -> datetime:
    """pull max date from the modifiedDateTime key in NWEA result set"""
    return max(result[filter_col] for result in result_set)


def pull_records_from_api_response(
    api_data: dict, max_date: datetime, filter_col: str = "testResults"
):
    """Pull the usable records from the API response"""

    if max_date is not None:
        if not is_datetime(max_date):
            # WARNING: this might be buggy if the date format changes
            max_date = format_date_string(max_date)
            max_date = convert_str_to_date(max_date)
        api_records = filter_based_on_max_dates(api_data[filter_col], max_date)
    else:
        api_records = api_data[filter_col]

    return api_records


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
