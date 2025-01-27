"""
Validators for columns in a dataframe
"""
import logging

import pandas as pd

from etl.validators._base import _BaseValidator


class ColumnNameValidator(_BaseValidator):
    """Validator for column names
    
    Example Usage
    -------------
        >>> from validators.column_validators import ColumnNameValidator
        >>> data = pd.DataFrame({'col1': [1, 2, 3, 4], 'col2': [5, 6, 7, 8]})
        >>> validator = ColumnNameValidator(['col1', 'col2'])
        >>> validator.validate(data)
        >>> validator.validated_
        True
    """

    def __init__(self, required_columns: list):
        """Initialize the class
        Args:
            required_columns (list): List of required columns
        """
        self.required_columns = required_columns
        self.validated_ = False

    def _validate(self, data: pd.DataFrame):
        """Validate the column names
        Args:
            data (pd.DataFrame): Data to validate
        """

        # Check if the required columns are in the data
        columns = data.columns.tolist()
        for column in self.required_columns:
            if column not in columns:
                logging.error(f"Column {column} is required but not found in the data")
                raise ValueError(
                    f"Column {column} is required but not found in the data"
                )

        # Check if there are any columns in the data that are not required
        for column in columns:
            if column not in self.required_columns:
                logging.warning(
                    f"Column {column} is not required but found in the data"
                )
                raise ValueError(
                    f"Column {column} is not required but found in the data"
                )

        self.validated_ = True


class ColumnTypeValidator(_BaseValidator):
    """
    Validator for column types


    Example Usage
    -------------
        >>> from validators.column_validators import ColumnTypeValidator
        >>> data = pd.DataFrame({'col1': [1, 2, 3, 4], 'col2': ['5', '6', '7', '8']})
        >>> validator = ColumnTypeValidator({'col1': int, 'col2': str})
        >>> validator.validate(data)
        >>> validator.validated_
        True
    """

    def __init__(self, col_types: dict):
        """Initialize the class
        Args:
            col_types (dict): Mapping of column names to types
        """
        self.col_types = col_types
        self.validated_ = False

    def _validate(self, data: pd.DataFrame):
        """Validate the column types
        Args:
            data (pd.DataFrame): Data to validate
        """
        for col, col_type in self.col_types.items():
            if data[col].dtype != col_type:
                logging.error(f"Column {col} is not of type {col_type}")
                raise ValueError(f"Column {col} is not of type {col_type}")

        self.validated_ = True


class DuplicateValueValidator(_BaseValidator):
    """
    Validator for duplicate values in a column

    Example Usage
    -------------
        >>> from validators.column_validators import DuplicateValueValidator
        >>> df = pd.DataFrame({'col1': [1, 2, 3, 4, 5, 5], 'col2': [6, 7, 8, 9, 5, 5]})
        >>> validator = DuplicateValueValidator(subset=['col1', 'col2'])
        >>> validator.validate(data)
        >>> validator.duplicates_
        [5]
        >>> validator.validated_
        False
    """

    def __init__(self, subset: list):
        """Initialize the class
        Args:
            subset (list): List of columns to check for duplicates
        """
        self.subset = subset
        self.duplicates_ = []
        self.validated_ = False

    def _validate(self, data: pd.DataFrame):
        """Validate the column types
        Args:
            data (pd.DataFrame): Data to validate
        """
        mask = data.duplicated(subset=self.subset)
        self.duplicates_ = data.loc[mask].values.tolist()
        if len(self.duplicates_) > 0:
            logging.error(f"Found duplicate values in columns {self.subset}")
            raise ValueError(f"Found duplicate values in columns {self.subset}")

        self.validated_ = True
