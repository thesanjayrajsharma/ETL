"""
Helper functions for the project.
"""
import os
import time
import requests
import logging

import pandas as pd

from datetime import datetime
from tqdm import tqdm

def generate_api_header(token_url: str = 'https://api.nwea.org/auth/v1/token',
                        grant_type = 'client_credentials'):
    
    """Get access token and generate api headers to use for API connections"""
    auth_code = os.environ.get('NWEA_AUTH')
    api_key   = os.environ.get('NWEA_API')
    
    token_headers = {
            'Authorization': auth_code,
            'apikey': api_key,
            'grant_type': grant_type,
            'Content-Length': '0'
    }
    
    # get auth token to later connect to the api
    try:
        resp_json    = requests.post(token_url, headers = token_headers).json()
        access_token = resp_json['access_token']
        api_headers  = {
            'Content/Type': 'application/json',
            'apikey': api_key,
            'Authorization': f'Bearer {access_token}'
        }

        return api_headers
    except Exception as e:
        logging.info(f"Could not connect to API because: {e}")
        return None 
    
def connect_to_api(api_url, api_headers, api_params):
    """Connect to API given certain parameters"""
    resp = requests.get(api_url, headers = api_headers, params = api_params)

    if resp.status_code != 504:
        return resp
    
    # 504 means the server is busy, so retry the connection
    return connect_to_api_with_504_error(api_url, api_headers, api_params)

def connect_to_api_with_504_error(api_url, api_headers, api_params, 
                                  resp = None, num_attempts = 1, wait_time = 5):
    """Code to continually try to connect to endpoint if given a 504 error"""
    if num_attempts >= 5:
        return resp
    
    logging.info(f"Waiting {wait_time} seconds to reconnect to the API due to 504 or 401 status code")
    time.sleep(wait_time)
    
    resp = requests.get(api_url, headers = api_headers, params = api_params)
    
    if resp.status_code == 504:
        # use recursion to continually call error until limit hits
        # wait 5 more seconds to retry after each unsuccessful attempt
        return connect_to_api_with_504_error(api_url, api_headers, api_params, resp = resp, 
                                             num_attempts = num_attempts + 1,
                                             wait_time = wait_time)
    
    else:
        return resp
    
def pull_records_from_api_response(api_data: dict, max_date: datetime):
    """Pull the usable records from the API response"""

    # done here to avoid circular import
    from etl.etl import (format_date_string, convert_str_to_date, 
                         filter_based_on_max_dates, is_datetime)

    if max_date is not None and type(max_date) == str:
        max_date = format_date_string(max_date)
        max_date = convert_str_to_date(max_date)
        api_records = filter_based_on_max_dates(api_data['testResults'], max_date)
    elif max_date is not None and is_datetime(max_date):
        api_records = filter_based_on_max_dates(api_data['testResults'], max_date)
    else:
        api_records = api_data['testResults']
        
    return api_records
    
def pull_data_from_api(api_resp, api_url, api_params, api_headers, max_date = None):
    """If successful 200 status code, pull data from api until no records are left"""
    api_pull_complete = False
    has_next_page = True
    
    api_data = api_resp.json()
    
    api_results  = []
    
    # in case we get a 504 timeout error
    error_dict = {}
    
    api_records = pull_records_from_api_response(api_data, max_date)
    
    api_results.extend(api_records)
    
    while not api_pull_complete:
        if not has_next_page:
            break
            
        has_next_page = api_data['pagination']['hasNextPage']
        
        if has_next_page:
            api_params['next-page'] = api_data['pagination']['nextPage']
            resp = connect_to_api(api_url = api_url, 
                                  api_headers = api_headers, 
                                  api_params = api_params)
            if resp.status_code != 200:
                error_dict = {
                    'school-bid': api_headers['school-bid'],
                    'error-type': 'api-error',
                    'message': resp.status_code,
                    'date': datetime.now()
                }
                return api_results, error_dict
                
            else:
                api_data = resp.json()
                api_records = pull_records_from_api_response(api_data, max_date)
                api_results.extend(api_records)
                has_next_page = api_data['pagination']['hasNextPage']
                
    return api_results, error_dict
                            
def pull_api_data_from_bid_list(bid_list: list, cutoffs: pd.DataFrame, 
                                api_url = 'https://api.nwea.org/test-results/v1/growth',
                                api_headers = None
                                ) -> tuple:
    """Function to pull in csv file with complete list of bids and parse api data from them"""

    if api_headers is None:
        raise ValueError("Could not generate API headers")
                       
    current_bids = cutoffs.index
                            
    result_set   = []

    tracking_set = []

    for bid in tqdm(bid_list):
        bid_currently_exists = bid in current_bids
        if bid_currently_exists:
            max_date = cutoffs.loc[bid]
        else:
            max_date = None
        api_params = {'school-bid': bid}
        logging.info(f"Pulling data for school with bid: {bid}")

        api_request_complete = False

        # pull from the API
        api_resp = connect_to_api(api_url, api_headers, api_params)

        if api_resp.status_code != 200:
            tracking_set.append({
                'school-bid': bid,
                'error-type': 'api-error',
                'message': api_resp.status_code,
                'date': datetime.now(),
                'added-records': False
            })

        else:
            api_data, error_dict  = pull_data_from_api(api_resp, api_url, api_params, 
                                                       api_headers, max_date = max_date)
            api_data              = pd.DataFrame(api_data)
            api_data['parentBid'] = bid

            logging.info(f"Added {api_data.shape[0]} new rows for bid: {bid}")

            if error_dict:
                tracking_set.append(error_dict)

            result_set.append(api_data)

    if not result_set:
        result_set.append(pd.DataFrame())
    
    results_data = pd.concat(result_set)
    tracking_data = pd.DataFrame(tracking_set)
    
    return results_data, tracking_data