import logging
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import datetime
import os
import time
import pandas as pd
from prefect_email import EmailServerCredentials, email_send_message
import requests
from src.env_config import *
sys.path.append(ROOT_DIR)
from scripts.oracledb_connection.internals.db import *
from datetime import datetime 
from prefect import flow, task


def generate_timestamp():
    return datetime.now().strftime("%d-%m-%Y")


'''gets district Bearer and oneroster application id'''


def get_district_key():
    url = 'https://oneroster-proxy.classlink.io/applications'
    headers = {
        'Authorization': F'Bearer {CLASSLINK_BEARER}'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()['applications']

        df = pd.json_normalize(data)
        distrit_tokens = df[['bearer', 'oneroster_application_id']]
        logging.info("Total district tokens:%d",len(distrit_tokens))
        return distrit_tokens




'''gets all the classlink students'''

def get_all_classlink_students():
    distrit_tokens = get_district_key()
    
    df_all_students = pd.DataFrame()

    for index, row in distrit_tokens.iterrows():
        bearer = row['bearer']
        oneroster_application_id = row['oneroster_application_id']
        offset = 0
        
        while True:
            url = f'https://oneroster-proxy.classlink.io/{oneroster_application_id}/ims/oneroster/v1p1/students/?limit=10000&offset={offset}&orderBy=asc'
            headers = {
                'Authorization': f'Bearer {bearer}'
            }

            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()['user']

                if not data:
                    logging.info(f"No data for oneroster_application_id {oneroster_application_id}")
                    break

                df_temp = pd.json_normalize(data)
                df_temp['bearer'] = bearer
                df_temp['oneroster_application_id'] = oneroster_application_id 

                df_all_students = pd.concat([df_all_students, df_temp], ignore_index=True)

                if len(data) < 10000:
                    break

                offset += 10000
            else:
                logging.info(f"Request failed with status code {response.status_code}")
                break

    if not  os.path.exists(CLASSLINK_ALL_STUDENTS):
        os.makedirs(CLASSLINK_ALL_STUDENTS)

    folder_path = CLASSLINK_ALL_STUDENTS
    filename = f"{folder_path}classlink_all_student_ids_{generate_timestamp()}.csv"
    df_all_students.to_csv(filename, mode='a', header=True, index=False)

    return df_all_students




'''get students from Query DB'''
def get_classlink_students_from_query():
    query='''SELECT DISTINCT
    fsd2.district,
    fsd2.district_id,
    fsd2.school_name,
    fsd2.school_id,
    fsd2.class_link_source_id,
    fsd2.class_link_tenant_id,
    fsd1.student_first_name,
    fsd1.student_last_name,
    fsd1.grade,
    fsd1.class_link_source_id AS student_classlink_source_id
FROM
    dwh_data.fev_session_student_f fssf
    LEFT JOIN dwh_data.fev_session_d         fsd ON fssf.session_skey = fsd.session_skey
    LEFT JOIN dwh_data.fev_session_f         fsf ON fssf.session_skey = fsf.session_skey
    JOIN dwh_data.fev_student_d         fsd1 ON fssf.student_skey = fsd1.student_skey AND fsd1.class_link_source_id IS NOT NULL
    JOIN dwh_data.fev_tutor_d           ftd ON fssf.tutor_skey = ftd.tutor_skey
    JOIN dwh_data.fev_grade_d           gd ON fsd1.grade_id = gd.grade_id
    JOIN dwh_data.fev_program_d         fpd ON fsf.program_skey = fpd.program_skey
    JOIN dwh_data.fev_program_ver_d     fpvd ON fsf.program_ver_skey = fpvd.program_ver_skey
    JOIN dwh_data.fev_school_d          fsd2 ON fsd1.school_id = fsd2.school_id'''
    Classlink_Students=  execute_query_to_df(query)
    logging.info("Total ids returned by the query:%d",len(Classlink_Students))
    return Classlink_Students.drop_duplicates()
        



'''prepares student id ,bearer,onersoter application id combinations to pass to the API'''

def get_final_classlink_students():
    df1 = get_classlink_students_from_query() 
    df2=get_all_classlink_students()

    merged_df = pd.merge(df1, df2, left_on='student_classlink_source_id', right_on='sourcedId', how='left')
    filtered_df = merged_df[merged_df['bearer'].notna()]
    filtered_df_null = merged_df[merged_df['bearer'].isna()]

    '''students to get data for from API'''
    valid_student_ids = filtered_df[['student_classlink_source_id', 'bearer', 'oneroster_application_id', 'username']]

    '''student ids from query not in the list  of students ids from classlink api'''
    bearer_null = filtered_df_null[['student_classlink_source_id', 'bearer', 'oneroster_application_id', 'username']]

    if not os.path.exists(CLASSLINK_API_STUDENT_IDS):
        os.makedirs(CLASSLINK_API_STUDENT_IDS)

    folder_path = CLASSLINK_API_STUDENT_IDS

    filename = f"{folder_path}classlink_api_student_ids_{generate_timestamp()}.csv"
    valid_student_ids.to_csv(filename, mode='a', header=True, index=False)

    return valid_student_ids




'''gets classes for each student id'''
@task
def run_classes_data_pull():

    if not os.path.exists(CLASSLINK_ERROR_LOGS):
        os.makedirs(CLASSLINK_ERROR_LOGS)
    if not os.path.exists(CLASSLINK_BULK_DATA):
        os.makedirs(CLASSLINK_BULK_DATA)

    folder_path_for_data = CLASSLINK_BULK_DATA
    folder_path_for_error = CLASSLINK_ERROR_LOGS

       # Define the filename with timestamp
    filename_for_data = f"{folder_path_for_data}classlink_data_{generate_timestamp()}.csv"
    filename_for_error = f"{folder_path_for_error}classlink_error_log_{generate_timestamp()}.csv"

    df_students =get_final_classlink_students()
    df_students = df_students.drop_duplicates().reset_index(drop=True)
    logging.info("total ids to process:%d",len(df_students))
    num_requests = 0
    df_errored_records = pd.DataFrame()

    for index, row in df_students.iterrows():

        bearer = row['bearer']
        oneroster_application_id = row['oneroster_application_id']
        student_id = row['student_classlink_source_id']

        logging.info(f"Processing student id: {index + 1}")  
        offset = 0
        while True:
            url = f'https://oneroster-proxy.classlink.io/{oneroster_application_id}/ims/oneroster/v1p1/students/{student_id}/classes?limit=1000&offset={offset}&orderBy=asc'
            headers = {'Authorization': f'Bearer {bearer}'}

            try:
                response = requests.get(url, headers=headers)
            except Exception as e:
                logging.error(f"An error occurred: {e}")
                break

            num_requests += 1
            if response.status_code == 200:
                data = response.json()['classes']

                if not data:
                    logging.info(f"No data for oneroster_application_id {oneroster_application_id}")
                    df_row = pd.DataFrame({
                    'CLASSLINK_STUDENT_ID': [student_id],
                    'LOAD_DATE': [datetime.now().strftime("%d-%m-%Y")],
                    'MESSAGE':"No data classes data,as student is not enrolled in any class"
                })
                    

                    df_row.to_csv(filename_for_error, mode='a', header=not os.path.isfile(filename_for_error), index=False)
                    break

                df_temp = pd.json_normalize(data)
                df_temp = df_temp[[
                    'sourcedId',
                    'status',
                    'title',
                    'dateLastModified',
                    'classCode',
                    'classType',
                    'location',
                    'grades',
                    'subjects',
                    'subjectCodes',
                    'course.sourcedId',
                    'school.sourcedId'
                ]]
                df_temp['student_classlink_source_id'] = student_id

                
                df_temp.to_csv(filename_for_data, mode='a', header=not os.path.isfile(filename_for_data), index=False)

                if len(data) < 1000:
                    break

                offset += 1000
            else:
                logging.error(f"Request failed with status code {response.status_code}")

                df_errored_records = pd.DataFrame({
                    'CLASSLINK_STUDENT_ID': [student_id],
                    'LOAD_DATE': [datetime.now().strftime("%d-%m-%Y")],
                    'MESSAGE': [response.status_code]
                })
                df_errored_records.to_csv(filename_for_error, mode='a', header=not os.path.isfile(filename_for_error), index=False)

                if response.status_code == 403 or response.status_code == 429:
                    logging.info(f"Encountered {response.status_code} response. Sleeping for 5 minutes.")
                    time.sleep(600)
                else :
                    logging.error(f"Encountered {response.status_code} response")
                    break
   


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
    
    # Construct the full path to the latest file
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




'''extract new data by comparing with the last pulled data file'''

@task
def get_incremental_data():

    
    folder_path=CLASSLINK_BULK_DATA
    new_data_folder=CLASSLINK_INCREMENTAL_DATA
    
    if not os.path.exists(CLASSLINK_INCREMENTAL_DATA):
        os.makedirs(CLASSLINK_INCREMENTAL_DATA)

    new_data_file = f"{new_data_folder}classlink_data_{generate_timestamp()}.csv"

    
    df_new_data =pd.read_csv(get_latest_file_from_folder(folder_path,file_prefix='classlink_data_'))
    last_pulled_file = get_second_latest_file_from_folder(folder_path,file_prefix='classlink_data_')

    ''' First run scenario, no previous data, treat all current data as new'''
    if last_pulled_file is None:
        
        logging.info("First run, treating all data as new.")
        df_new_data = df_new_data.drop_duplicates()
        df_new_data.to_csv(new_data_file, index=False)
        return df_new_data
    

    last_pulled_data = pd.read_csv(last_pulled_file)
    '''drop duplicates if any'''
    df_new_data=df_new_data.drop_duplicates()
    last_pulled_data=last_pulled_data.drop_duplicates()

    merged_data = pd.merge(df_new_data, last_pulled_data, on=['sourcedId', 'student_classlink_source_id'], suffixes=('_new', '_old'), how='left')

    updates = merged_data[merged_data['dateLastModified_new'] > merged_data['dateLastModified_old']]
    new_inserts = merged_data[merged_data['dateLastModified_old'].isna()]

    modified_or_new_data = pd.concat([updates, new_inserts])
    modified_or_new_data = modified_or_new_data.drop(columns=[col for col in merged_data.columns if '_old' in col])

    modified_or_new_data.columns = [col.split('_')[0] if '_new' in col else col for col in modified_or_new_data.columns]
    modified_or_new_data=modified_or_new_data[[
                    'sourcedId',
                    'status',
                    'title',
                    'dateLastModified',
                    'classCode',
                    'classType',
                    'location',
                    'grades',
                    'subjects',
                    'subjectCodes',
                    'course.sourcedId',
                    'school.sourcedId',
                    'student_classlink_source_id'
                ]]
    modified_or_new_data.to_csv(new_data_file, index=False)


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
    CLASSLINK_ALL_STUDENTS:'classlink_all_student_',
    CLASSLINK_API_STUDENT_IDS:'classlink_api_student_',
    CLASSLINK_BULK_DATA:'classlink_data_',
    CLASSLINK_ERROR_LOGS:'classlink_error_log_',
    CLASSLINK_INCREMENTAL_DATA:'classlink_data_'
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
