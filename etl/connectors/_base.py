"""
Contains base classes for connectors to external systems:  API's, databases, etc.
"""
import time
import os
import logging
from abc import ABCMeta, abstractmethod
import requests

import pandas as pd


class _BaseConnector(metaclass=ABCMeta):
    """Base class for all connectors"""

    def __init__(self, return_data = False):
        """Initialize the class
        Args:
            return_data (bool): Whether or not to return the data
        """
        self.return_data = return_data

    def connect(self):
        """Connect to the external system"""
        return self._connect()
    
    @abstractmethod
    def _connect(self):
        """Connect to the external system"""
        pass

    @abstractmethod
    def pull_data(self):
        """Pull the data"""
        pass


class _BaseAPIConnector(_BaseConnector):
    """Base class for API connectors"""

    def __init__(
        self,
        url: str,
        params: dict = None,
        headers: dict = None,
        retry_https_codes: list = [],
        return_data = True
    ):
        """Initialize the class
        Args:
            url (str): URL to connect to
            params (dict): Parameters to pass to the API
            headers (dict): Headers to pass to the API
            retry_https_codes (list): api status codes that will invoke another api request
            return_data (bool): Whether or not to return the data from pull_data()
        """

        super().__init__(return_data)

        self.url = url
        self.params = params
        self.headers = headers
        self.retry_https_codes = retry_https_codes

    @abstractmethod
    def _validate_response(self):
        """Validate the response from the API"""
        pass

    @abstractmethod
    def _parse_response(self):
        """Parse the response from the API"""
        pass

    @abstractmethod
    def _authenticate(self):
        """Authenticate with the API"""
        pass

    @abstractmethod
    def _paginate_response(self):
        """Paginate through the API response to retrieve all entries"""
        pass

    @abstractmethod
    def _filter_data(self):
        """Filter the data from API response"""
        pass

    def _connect(self) -> None:
        """Private method to connect to the API

        Returns:
            None
        """
        self._authenticate()
        self.req = requests.get(self.url, params=self.params, headers=self.headers)

    def _recursive_api_connect(
        self,
        max_attempts: int = 5,
        num_attempts: int = 0,
        wait_time: int = 1,
        time_added: int = 0,
    ) -> None:
        """
        Repeatedly try to connect to an API if there is a connection issue
        w/ recursion

        Args:
            max_attempts (int): # of times to try and connect
            num_attempts (int): counter for current # of tries, should stay at 1
            wait_time (int): how long to wait inbetween connections
            time_added (int): additional time to add between each attempt
        """
        # updates stopping criteria for recursion
        wait_time += time_added
        num_attempts += 1

        print(f"Attempt #{num_attempts} to connect to API")
        if num_attempts >= max_attempts:
            # won't be used, just here to make sure the recursion stops
            return None

        logging.info(
            f"""Waiting {wait_time} seconds to reconnect to the API due
                     to {self.req.status_code} status code"""
        )
        time.sleep(wait_time)

        self._connect()

        if self.req.status_code in self.retry_https_codes:
            self._recursive_api_connect(wait_time=wait_time, 
                                        time_added=time_added,
                                        num_attempts=num_attempts)

class FileConnector(_BaseConnector):
    """Loads data from a file
    
    Example Usage
    -------------
        >>> from connectors import FileConnector
        >>> connector = FileConnector(
        >>>     method = 'pandas',
        >>>     file_path = 'data/parent_bids.csv')
        >>> connector.pull_data()
        >>> connector.pulled_data_
            PARENT_BID MAX(MODIFIED_DATETIME)
            0 00000000-0000-0000-0000-000000000000 2020-12-15 13:09:00
    """

    def __init__(self, method: str, file_path: str, return_data = False):
        """Initialize the class
        Args:
            method (str): method to use to load the data, one of (pandas, raw, json)
            file_path (str): Path to the file to load
        """
        super().__init__(return_data)
        self.method = method
        self.file_path = file_path

    def _connect(self) -> None:
        """Private method to connect to the file
        Skipping this for now, since not relevant to a file

        Returns:
            None
        """
        pass

    def pull_data(self, **load_kwargs) -> None:
        """Pull the data from the file
        Args:
            **load_kwargs: Keyword arguments to pass to the pandas read_csv function
        Returns:
            None
        """
        self._validate_file_path()
        self._load_file(**load_kwargs)

        if self.return_data:
            return self.pulled_data_

    def _load_file(self, **load_kwargs) -> None:
        """Load the data from the file
        Args:
            file_path (str): Path to the file to load
            **load_kwargs: Keyword arguments to pass to the pandas read_csv function
        Returns:
            None
        """

        if self.method not in ["pandas", "raw", "json"]:
            raise ValueError(
                f"Invalid method: {self.method}, must be one of pandas, raw, json"
            )

        if self.method == "pandas":
            self._load_pandas(**load_kwargs)

        elif self.method == "json":
            self._load_json()

        else:
            self._load_raw()

    def _load_pandas(self, **load_kwargs) -> pd.DataFrame:
        """Load the data using pandas
        Args:
            **load_kwargs: Keyword arguments to pass to the pandas read_csv function

        Returns:
            pandas.DataFrame: DataFrame containing the loaded data
        """

        self.pulled_data_ = pd.read_csv(self.file_path, **load_kwargs)

    def _load_raw(self) -> str:
        """Load the data as raw text
        Returns:
            str: Raw text of the file
        """

        with open(self.file_path, "r") as f:
            self.pulled_data_ = f.read()

    def _load_json(self) -> dict:
        """Load the data as raw text
        Returns:
            dict: JSON of the file
        """

        with open(self.file_path, "r") as f:
            self.pulled_data_ = json.load(f)

    def _validate_file_path(self) -> None:
        """Validate the file path"""
        if not self.file_path:
            raise ValueError("File path is required")

        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        if not os.path.isfile(self.file_path):
            raise ValueError(f"File path is not a file: {self.file_path}")

        if not os.access(self.file_path, os.R_OK):
            raise PermissionError(f"File path is not readable: {self.file_path}")

        if os.path.getsize(self.file_path) == 0:
            raise ValueError(f"File is empty: {self.file_path}")

        if not os.path.splitext(self.file_path)[1] in [".csv", '.txt', '.json']:
            raise ValueError(f"File is not a CSV, TXT, or JSON: {self.file_path}")
        
class _DatabaseConnector(_BaseConnector):
    """Base class for all database Connectors"""

    def __init__(self, connection_string: str, return_data = False):
        """Initialize the class
        Args:
            connection_string (str): Connection string to use to connect to the database
            return_data (bool): Whether or not to return the data from pull_data()
        """

        super().__init__(return_data)
        self.connection_string = connection_string

    def load(self, query: str, **load_kwargs) -> None:
        """Load the data from the database
        Args:
            query (str): Query to run against the database
            **load_kwargs: Keyword arguments to pass to the pandas read_sql function
        Returns:
            None
        """

        self._validate_query(query)

        self.data_ = pd.read_sql(query, self._get_connection(), **load_kwargs)

    def _validate_query(self, query: str) -> None:
        """Validate the query
        Args:
            query (str): Query to validate
        Returns:
            None
        """

        if not query:
            raise ValueError("Query is required")

        if not isinstance(query, str):
            raise TypeError(f"Query must be a string: {query}")

        if not query.strip():
            raise ValueError("Query cannot be blank")

    def _get_connection(self) -> "sqlalchemy.engine.base.Engine":
        """Get a connection to the database
        Returns:
            sqlalchemy.engine.base.Engine: Connection to the database
        """
        from sqlalchemy import create_engine

        return create_engine(self.connection_string)
