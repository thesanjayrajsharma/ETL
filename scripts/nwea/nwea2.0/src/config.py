import logging
from prefect import task
from prefect_email import EmailServerCredentials, email_send_message
from src.env_config import *
import os
from datetime import datetime, timedelta

data_cols_config = {
    'testResultBid':'TEST_RESULT_BID',
    'studentBid':'STUDENT_BID',
    'schoolBid':'SCHOOL_BID',
    'termBid':'TERM_BID',
    'subjectArea':'SUBJECT_AREA',
    'grade':'GRADE',
    'testName':'TEST_NAME',
    'testKey':'TEST_KEY',
    'testType':'TEST_TYPE',
    'growthEventYn':'GROWTH_EVENT_YN',
    'duration':'DURATION',
    'status':'STATUS',
    'rit':'RIT',
    'standardError':'STANDARD_ERROR',
    'ritScoreHigh':'RIT_SCORE_HIGH',
    'ritScoreLow':'RIT_SCORE_LOW',
    'responseDisengagedPercentage':'RESPONSE_DISENGAGED_PERCENTAGE',
    'impactOfDisengagement':'IMPACT_OF_DISENGAGEMENT',
    'administrationStartDateTime':'ADMINISTRATION_START_DATE_TIME',
    'administrationEndDateTime':'ADMINISTRATION_END_DATE_TIME',
    'modifiedDateTime':'MODIFIED_DATE_TIME',
    'ITEMS_SHOWN':'ITEMS_SHOWN',
    'ITEMS_CORRECT':'ITEMS_CORRECT',
    'ITEMS_TOTAL':'ITEMS_TOTAL',
    'NORMS_PERCENTILE':'NORMS_PERCENTILE',
    'QUANTILE_SCORE':'QUANTILE_SCORE',
    'QUANTILE_MAX':'QUANTILE_MAX',
    'QUANTILE_MIN':'QUANTILE_MIN',
    'QUANTILE_RANGE':'QUANTILE_RANGE',
    'QUANTILE_ORIGINAL':'QUANTILE_ORIGINAL',
    'INSTRUCTIONAL_AREA_BID':'INSTRUCTIONAL_AREA_BID',
    'INSTRUCTIONAL_AREA_NAME':'INSTRUCTIONAL_AREA_NAME',
    'INSTRUCTIONAL_AREA_SCORE':'INSTRUCTIONAL_AREA_SCORE',
    'INSTRUCTIONAL_AREA_STD_ERR':'INSTRUCTIONAL_AREA_STD_ERR',
    'INSTRUCTIONAL_AREA_LOW':'INSTRUCTIONAL_AREA_LOW',
    'INSTRUCTIONAL_AREA_HIGH':'INSTRUCTIONAL_AREA_HIGH',
    'LEXILE_SCORE':'LEXILE_SCORE',
    'LEXILE_MIN':'LEXILE_MIN',
    'LEXILE_MAX':'LEXILE_MAX',
    'LEXILE_RANGE':'LEXILE_RANGE',
    'parentBid':'PARENT_SCHOOL_BID',
    'accommodations': 'ACCOMMODATIONS'
}

student_cols_config = {
    'dateOfBirth': 'DATE_OF_BIRTH',
    'districtBid': 'DISTRICT_BID',
    'ethnicity': 'ETHNICITY',
    'ethnicityCustom': 'ETHNICITY_CUSTOM',
    'firstName': 'FIRST_NAME',
    'gender': 'GENDER',
    'grade': 'GRADE',
    'gradeCustom': 'GRADE_CUSTOM',
    'lastName': 'LAST_NAME',
    'middleName': 'MIDDLE_NAME',
    'schoolBid': 'SCHOOL_BID',
    'studentId': 'STUDENT_ID', 
    'stateStudentId': 'STATE_STUDENT_ID',
    'studentBid': 'STUDENT_BID'
}

dtype_config = {
        'TEST_RESULT_BID': 'str',
        'STUDENT_BID': 'str',
        'SCHOOL_BID': 'str',
        'TERM_BID': 'str',
        'SUBJECT_AREA': 'str',
        'GRADE': 'str',
        'TEST_NAME': 'str',
        'TEST_KEY': 'str',
        'TEST_TYPE': 'str',
        'GROWTH_EVENT_YN': 'str',
        'DURATION': 'float',
        'STATUS': 'str',
        'RIT': 'float',
        'STANDARD_ERROR': 'float',
        'RIT_SCORE_HIGH': 'float',
        'RIT_SCORE_LOW': 'float',
        'RESPONSE_DISENGAGED_PERCENTAGE': 'float',
        'IMPACT_OF_DISENGAGEMENT': 'str',
        'ACCOMMODATIONS_NAME': 'str',
        'ACCOMMODATIONS_CATEGORY': 'str',
        'ACCOMMODATIONS':'str',
        'PARENT_BID': 'str',
        'ITEMS_SHOWN': 'float',
        'ITEMS_CORRECT': 'float',
        'ITEMS_TOTAL': 'float',
        'PERCENTILE': 'float',
        'REFERENCE': 'str',
        'TYPE': 'str',
        'QUANTILE_SCORE': 'str',
        'QUANTILE_MAX': 'str',
        'QUANTILE_MIN': 'str',
        'QUANTILE_RANGE': 'str',
        'QUANTILE_ORIGINAL': 'str',
        'INSTURCTIONAL_AREA_BID': 'str',
        'INSTRUCTIONAL_AREA_NAME': 'str',
        'INSTRUCTIONAL_AREA_SCORE': 'float',
        'INSTRUCTIONAL_AREA_STD_ERR': 'float',
        'INSTRUCTIONAL_AREA_LOW': 'float',
        'INSTRUCTIONAL_AREA_HIGH': 'float',
        'LEXILE_SCORE': 'str',
        'LEXILE_MIN': 'str',
        'LEXILE_MAX': 'str',
        'LEXILE_RANGE': 'str',
        'ADMINISTRATION_START_DATE_TIME':'str',
        'ADMINISTRATION_END_DATE_TIME':'str',
        'MODIFIED_DATE_TIME':'str'
    } 

def setup_logging():
    for handler in logging.getLogger().handlers[:]:
        logging.getLogger().removeHandler(handler)

# Configure the root logger
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
    )


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


@task
def delete_old_files():
    folder_prefix_map = {
        BULK_DATA_FILE_PATH:'assessment_results_',
        INCREMENTAL_DATA_FILE_PATH:'assessment_results_incremental_',
        ERRORS_FILE_PATH: 'final_errors_',
        META_DATA_FILE_PATH:'meta_data',
    
        STUDENT_DATA_PATH:'student_data_',
        STUDENT_ERRORS_PATH:'student_errors_',
        STUDENT_BIDS_PATH:'student_bids_',
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


