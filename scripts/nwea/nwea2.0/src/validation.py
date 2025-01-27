"""Functions to Validate Data in ETL STEP"""
import logging
import pandas as pd

from datetime import datetime

from .config import dtype_config

def validate_data(data: pd.DataFrame, dtype_config: dict, max_date: datetime) -> None:
    """Validate the data"""
    logging.info("Validating data")
    for key, value in dtype_config.items():
        logging.info(f"Validating {key} column")
        if value == 'str':
            valid_data_type_check(data, [str], key)
            duplicates_check(data, [key])
        elif value == 'date':
            valid_data_type_check(data, [datetime], key)
            valid_date_check(data, max_date, key)
        elif value == 'int':
            valid_data_type_check(data, [int], key)
        elif value == 'float':
            valid_data_type_check(data, [float], key)
        elif value == 'bool':
            valid_data_type_check(data, [bool], key)

    duplicates_check(data,  subset = None)

def duplicates_check(data: pd.DataFrame, subset: list[list, None]) -> None:
    """Check for duplicates in the data"""
    if subset is None:
        subset = data.columns.tolist()
    if data.duplicated(subset = subset).sum() > 0:
        logging.error(f"Duplicate values found in {subset} column")
        raise ValueError(f"Duplicate values found in {key} column")

def valid_date_check(data: pd.DataFrame, max_date: datetime, date_col: str) -> None:
    """Check for valid dates in the data"""
    if data[date_col].max() < max_date:
        logging.error(f"Date found in {date_col} column is greater than max date")
        raise ValueError(f"Date found in {date_col} column is greater than max date")

def valid_data_type_check(data: pd.DataFrame, valid_values: list, key: str) -> None:
    """Check for valid values in the data"""
    if not data[key].isin(valid_values).all():
        logging.error(f"Invalid value found in {key} column")
        raise ValueError(f"Invalid value found in {key} column")

def valid_data_type_check(data: pd.DataFrame, valid_types: list, key: str) -> None:
    """Check for valid data types in the data"""
    if not data[key].dtype in valid_types:
        logging.error(f"Invalid data type found in {key} column with value {data[key].dtype}")
        raise ValueError(f"Invalid data type found in {key} column with value {data[key].dtype}")

        