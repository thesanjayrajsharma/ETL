"""
Connector for the NWEA API, for both assessment and student data
"""
import requests
import os
import logging

from datetime import datetime
from dotenv import load_dotenv

import pandas as pd

from etl.connectors._base import _BaseAPIConnector
from etl.connectors._utils import (
    pull_records_from_api_response,
    filter_based_on_max_dates,
)


# need to import .env from the root directory
# TO-DO:  turn this into a function that can be called
current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)
base_dir = os.path.dirname(os.path.dirname(current_dir))
env_path = os.path.join(base_dir, ".env")
load_dotenv(env_path)


class NWEAAssessmentConnector(_BaseAPIConnector):
    """Connector for the NWEA Assessment API

    Example Usage
    -------------
    >>> connector = NWEAAssessmentConnector(
        url="https://api.nwea.org/test-results/v1/growth",
        params={"school-bid": "2c195342-9ea0-410c-afb5-0713dd0e6e0a"},
        bid="2c195342-9ea0-410c-afb5-0713dd0e6e0a",
        max_date=pd.to_datetime("2021-09-23")
    )

    # NOTE:  using pd.to_datetime is suggested, so you can compare
    # datetime objects with np.datetime64 objects

    # this returns data from the API in a dataframe for given bid
    >>> data = connector.pull_data()

    >>> data
    >>> bid  ...  modifiedDateTime
    >>> 0  test_bid  ...  2021-09-23 19:56:55.000
    """

    def __init__(
        self,
        url: str,
        params: dict,
        bid: str,
        headers: dict = None,
        max_date: datetime = None,
        token_url: str = "https://api.nwea.org/auth/v1/token",
        grant_type: str = "client_credentials",
        retry_https_codes: list = [504, 404],
        return_data=True,
    ):
        """Initialize the class
        Args:
            url (str): URL to connect to
            params (dict): Parameters to pass to the API
            bid (str): school bid to pull data for
            headers (dict): Headers to pass to the API
            max_date (datetime): Max date to pull data for
            token_url (str): URL to connect for authentication
            grant_type (str): Type of grant to use for authentication
        """
        super().__init__(url, params, headers, retry_https_codes, return_data)
        self.bid = bid
        self.max_date = max_date
        self.token_url = token_url
        self.grant_type = grant_type

    def _validate_response(self) -> None:
        """Validate the response from the API, meant to
        check for different API response errors, recursively
        reconnect with supplied status codes
        Returns:
            None
        """

        # retry connection if status code is in retry_https_codes
        if self.req.status_code in self.retry_https_codes:
            print("Connection error, retrying...")
            self._recursive_api_connect()
        elif self.req.status_code == 401:
            self._authenticate()
            self._connect()
            self._validate_response()
        elif self.req.status_code != 200:
            logging.error(
                f"Could not connect to API. Status code: {self.req.status_code}"
            )
            # to do:  export error to database
            self.error_ = {
                "school-bid": self.bid,
                "error-type": "api-error",
                "message": self.req.status_code,
                "date": datetime.now(),
            }

            raise ValueError(
                f"Could not connect to API. Status code: {self.req.status_code}"
            )

    def _parse_response(self) -> None:
        """Parse the response from the API
        Returns:
            None
        """
        self.json_response_ = self.req.json()

    def pull_data(self) -> None:
        """Pull the data from the API
        Returns:
            None
        """
        self._connect()
        self._validate_response()
        if self.req.status_code == 200:
            self._parse_response()
            self._paginate_response()
            self._filter_data()
            self._format_data()
            if self.return_data:
                return self.pulled_data_

        else:
            return pd.DataFrame()
            logging.error(
                f"Could not connect to API. Status code: {self.req.status_code}"
            )

    def _filter_data(self) -> None:
        """Filter the data
        Returns:
            None
        """
        self.api_results_ = filter_based_on_max_dates(self.api_results_, self.max_date)

    def _format_data(self) -> None:
        """Format the data returned from the API into a dataframe
        Returns:
            None
        """
        self.pulled_data_ = pd.DataFrame(self.api_results_)
        self.pulled_data_["modifiedDateTime"] = pd.to_datetime(
            self.pulled_data_["modifiedDateTime"]
        )
        # done to keep track of which school bid the data is for
        # multiple bids can be pulled for a single bid passed to API
        self.pulled_data_["PARENT_SCHOOL_BID"] = self.bid

    def _paginate_response(self):
        """Paginate through the API response to retrieve all entries
        Returns:
            None
        """
        api_pull_complete = False
        has_next_page = True

        self.api_results_ = []

        # have to do this now because we've already connected to the api
        api_records = pull_records_from_api_response(self.json_response_, self.max_date)

        self.api_results_.extend(api_records)

        while not api_pull_complete:
            if not has_next_page:
                break

            has_next_page = self.json_response_["pagination"]["hasNextPage"]

            if has_next_page:
                self.params["next-page"] = self.json_response_["pagination"]["nextPage"]
                self.connect()
                self._validate_response()
                self._parse_response()
                api_records = pull_records_from_api_response(
                    self.json_response_, self.max_date
                )
                self.api_results_.extend(api_records)

    def _authenticate(self) -> None:
        """Authenticate with the API
        Returns:
            None
        """
        auth_code = os.environ.get("NWEA_AUTH")
        api_key = os.environ.get("NWEA_API")

        # steps to authenticate w/ NWEA API:
        token_headers = {
            "Authorization": auth_code,
            "apikey": api_key,
            "grant_type": self.grant_type,
            "Content-Length": "0",
        }

        # get auth token to later connect to the api
        try:
            resp_json = requests.post(self.token_url, headers=token_headers).json()
            access_token = resp_json["access_token"]
            self.headers = {
                "Content/Type": "application/json",
                "apikey": api_key,
                "Authorization": f"Bearer {access_token}",
            }

        except Exception as e:
            logging.info(f"Could not connect to API because: {e}")
            return None


class NWEAStudentConnector(_BaseAPIConnector):
    pass
