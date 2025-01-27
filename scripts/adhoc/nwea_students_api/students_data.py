# Jonathan's function (generate_api_header), using to get Authorization
import logging
import requests
import pandas as pd
from src.env_config import NWEA_API_KEY,NWEA_AUTH
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
 
    
def get_schools_data():
    url = "https://api.nwea.org/organizations/v1/schools"
    headers = {
        "apikey": "MVY9krgNcqQAZJMw0Xl5vkstvjqpRvth",
        "Authorization": generate_api_header()['Authorization'],
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        schools = response.json()
        df = pd.DataFrame(schools)
        return df
    else:
        logging.info(f"Failed to retrieve school data, status code: {response.status_code}")
        return None


def get_students_data():
    all_district_school_df = get_schools_data()
    if all_district_school_df is None:
        logging.info("Could not retrieve schools data")
        return None
    
    school_bids=all_district_school_df['schoolBid']
    base_url = "https://api.nwea.org/organizations/v1/schools/{}/students"
    headers = {
        "apikey": "MVY9krgNcqQAZJMw0Xl5vkstvjqpRvth",
        "Authorization": generate_api_header()['Authorization'],
        "Content-Type": "application/json"
    }
    
    all_students = []
    
    for school_bid in school_bids:
        response = requests.get(base_url.format(school_bid), headers=headers)
        print(response.status_code)
        if response.status_code == 200:
            students = response.json()
            all_students.extend(students)
        else:
            logging.info(f"Failed to retrieve data for schoolBid: {school_bid}, status code: {response.status_code}")
    
    df = pd.DataFrame(all_students)
    return df


if __name__=='__main__':
    get_students_data()