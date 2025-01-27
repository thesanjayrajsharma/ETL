import logging
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import datetime
import os
import pandas as pd
from prefect_email import EmailServerCredentials, email_send_message
import requests
from src.env_config import *
sys.path.append(ROOT_DIR)
from scripts.oracledb_connection.internals.db import *
from datetime import datetime 
from prefect import flow, task



timestamp = datetime.now().strftime("%d-%m-%Y")

@task
def get_districts_keys():

    url = 'https://clever.com/oauth/tokens'
    headers = {
        'Authorization': F'Basic {CLEVER_BEARER}'
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()['data']
        df = pd.json_normalize(data)
        df_at = df['access_token']
        return df_at



@task
def get_sections_data ():
    df_at =  get_districts_keys()
    df_responses = pd.DataFrame()
    failed_responses = []
    for i, token in enumerate(df_at):
        headers = {
            'Authorization': f'Bearer {token}'
        }   
        base_url = 'https://api.clever.com/v3.0/sections?limit=10000'
        next_url = base_url
        while next_url:
            response = requests.get(next_url, headers=headers)
            if response.status_code == 200:
                data = response.json()

                df = pd.json_normalize(data['data'])
    
                columns_to_explode = ["data.students"]
                for column in columns_to_explode:
                    if column in df.columns:
                        df[column] = df[column].apply(lambda x: x if isinstance(x, list) else [])
                        df = df.explode(column)
                        df = df.join(pd.json_normalize(df[column]).add_prefix(f'{column}_'))
                df_responses = pd.concat([df_responses, df], ignore_index=True)                
                next_url = next((link.get('uri') for link in data.get('links', []) if link.get('rel') == 'next'), None)
    
                if not next_url:
                    break  
                if not next_url.startswith("https://api.clever.com"):
                    next_url = "https://api.clever.com" + next_url 
    

            else:
                failed_responses.append({
                    'token': token,
                    'status_code': response.status_code,
                    'response_text': response.text
                })
                break 

    if failed_responses:
        if not os.path.exists(CLEVER_ERROR_LOGS):
            os.makedirs(CLEVER_ERROR_LOGS)
        failed_df = pd.DataFrame(failed_responses)
        failed_filename = f"{CLEVER_ERROR_LOGS}failed_responses_{timestamp}.csv"
        failed_df.to_csv(failed_filename, index=False)
    

    if not os.path.exists(CLEVER_BULK_DATA):
        os.makedirs(CLEVER_BULK_DATA)
    filename = f"{CLEVER_BULK_DATA}Clever_sections_data_{timestamp}.csv"
    df_responses.to_csv(filename, index=False)



def get_latest_file_from_folder(folder_path, file_prefix="", file_extension=".csv", date_format="%d-%m-%Y"):
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    
    # Filter files by prefix (if provided) and extension
    if file_prefix:
        files = [f for f in files if f.startswith(file_prefix)]
    files = [f for f in files if f.endswith(file_extension)]
    # Get today's date and format it
    today = datetime.today().strftime(date_format)
    today_file = f"{file_prefix}{today}{file_extension}"
    
    if today_file not in files:
        raise(f'No file found with current date matching: {today_file}')
    try:
        latest_file = max(files, key=lambda x: datetime.strptime(x.rsplit("_", 1)[-1].replace(file_extension, ''), date_format))
    except ValueError as e:
        logging.error(f"Error parsing date from filename: {e}")
        return None

    latest_file_path = os.path.join(folder_path, latest_file)

    return latest_file_path


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
    second_latest_file_path = os.path.join(folder_path, second_latest_file)

    return second_latest_file_path



@task
def get_incremental_data():


    '''First run scenario where there is no previous file to compare '''
    if not os.path.exists(CLEVER_INCREMENTAL_DATA):    
        df2 = pd.read_csv(get_latest_file_from_folder(CLEVER_BULK_DATA,file_prefix='Clever_sections_data_'))
        os.makedirs(CLEVER_INCREMENTAL_DATA)
        filename = f"{CLEVER_INCREMENTAL_DATA}Clever_sections_data_{timestamp}.csv"
        df2.to_csv(filename, index=False)
        return
    
    df2 = pd.read_csv(get_latest_file_from_folder(CLEVER_BULK_DATA,file_prefix='Clever_sections_data_'))
    df1 = pd.read_csv(get_second_latest_file_from_folder(CLEVER_BULK_DATA,file_prefix='Clever_sections_data_'))
    
    joined_df = pd.merge(df2[['data.id', 'data.students']], df1[['data.id', 'data.students']], 
                         on=['data.id', 'data.students'], how='left', indicator=True)
    
    no_match_df = joined_df[joined_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    
    final_df = pd.merge(df2, no_match_df[['data.id', 'data.students']], on=['data.id', 'data.students'])

    join_columns = ['data.id', 'data.students']
    compare_columns = ['data.course', 'data.district', 'data.grade', 'data.name',
                      'data.school', 'data.sis_id', 'data.subject',
                      'data.teacher', 'data.term_id','data.section_number','data.period']
    
    df_joined = pd.merge(df1, df2, on=join_columns, suffixes=('_file1', '_file2'))

    def try_convert_to_float(val):
        try:
            return float(val)
        except ValueError:
            return val

    def detect_differences(row, compare_columns):
        for col in compare_columns:
            val1 = row[col + '_file1']
            val2 = row[col + '_file2']
            
            val1 = try_convert_to_float(val1)
            val2 = try_convert_to_float(val2)
                
            if pd.isna(val1):
                val1 = 'NaN'
            if pd.isna(val2):
                val2 = 'NaN'        
            if val1 != val2:
                return True
        return False

    differences_mask = df_joined.apply(lambda row: detect_differences(row, compare_columns), axis=1)    
    df_differences = df_joined.loc[differences_mask, join_columns]    
    final_final = pd.merge(df2, df_differences[['data.id', 'data.students']], on=['data.id', 'data.students'])    
    merged_output = pd.concat([final_df, final_final])
    filename = f"{CLEVER_INCREMENTAL_DATA}Clever_sections_data_{timestamp}.csv"
    merged_output.to_csv(filename, index=False)




def send_email(subject: str, msg: str, email_to=RECEIVERS_EMAIL):
    email_server_credentials = EmailServerCredentials.load("failure-alert")  # Load the email credentials block

    # Send the email using the provided arguments
    email_subject = email_send_message(
        email_server_credentials=email_server_credentials,
        subject=subject,
        msg=msg,
        email_to=email_to,
    )

    return email_subject

def setup_logging():
    for handler in logging.getLogger().handlers[:]:
        logging.getLogger().removeHandler(handler)

# Configure the root logger
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
    )


@task
def delete_old_files():
    folder_prefix_map = {
        CLEVER_BULK_DATA: 'Clever_sections_data_',
        CLEVER_ERROR_LOGS: 'failed_responses_',
        CLEVER_INCREMENTAL_DATA: 'Clever_sections_data_'
    }

    # Calculate the cutoff date (2 weeks ago)
    cutoff_date = datetime.now() - timedelta(weeks=2)

    for directory, prefix in folder_prefix_map.items():
        if os.path.exists(directory):
            for filename in os.listdir(directory):
                if filename.startswith(prefix):
                    date_str = filename.split('_')[-1].split('.')[0]
                    try:
                        file_date = datetime.strptime(date_str, '%d-%m-%Y')  
                    except ValueError:
                        logging.warning(f"Skipping file with invalid date format: {filename}")
                        continue  

                    
                    if file_date < cutoff_date:
                        file_path = os.path.join(directory, filename)
                        os.remove(file_path)
                        logging.info(f'Deleted: {file_path}')
        else:
            logging.warning(f"Directory not found: {directory}")

    logging.info("Cleanup completed.")


