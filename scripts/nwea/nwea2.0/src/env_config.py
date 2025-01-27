import os
from dotenv import find_dotenv, load_dotenv


'''path to .env remains same across prod and dev'''
dotenv_path = '/home/fevdevadmin/etl_repo/ETL/scripts/nwea/nwea2.0/src/.env'

load_dotenv(dotenv_path)

NWEA_AUTH = str(os.environ.get('NWEA_AUTH'))
NWEA_API_KEY = str(os.environ.get('NWEA_API_KEY'))
TEMP_FILE_PATH=str(os.environ.get('TEMP_FILE_PATH'))
BULK_DATA_FILE_PATH=str(os.environ.get('BULK_DATA_FILE_PATH'))
INCREMENTAL_DATA_FILE_PATH=str(os.environ.get('INCREMENTAL_DATA_FILE_PATH'))
ERRORS_FILE_PATH=str(os.environ.get('ERRORS_FILE_PATH'))
META_DATA_FILE_PATH=str(os.environ.get('META_DATA_FILE_PATH'))
ROOT_DIR=str(os.environ.get('ROOT_DIR'))
SCHOOL_BIDS_PATH=str(os.environ.get('SCHOOL_BIDS_PATH'))



STUDENT_DATA_PATH=str(os.environ.get('STUDENT_DATA_PATH'))
STUDENT_TEMP_PATH=str(os.environ.get('STUDENT_TEMP_PATH'))
STUDENT_ERRORS_PATH=str(os.environ.get('STUDENT_ERRORS_PATH'))
STUDENT_BIDS_PATH=str(os.environ.get('STUDENT_BIDS_PATH'))
REGION=str(os.environ.get('REGION'))
RECEIVERS_EMAIL=str(os.environ.get('RECEIVERS_EMAIL'))
CONNECTION_STRING=str(os.environ.get('CONNECTION_STRING'))
CONTAINER=str(os.environ.get('CONTAINER'))

ASSESSMENT_DATA_BLOB_FOLDER=str(os.environ.get('ASSESSMENT_DATA_BLOB_FOLDER'))
ASSESSMENT_ERRORS_BLOB_FOLDER=str(os.environ.get('ASSESSMENT_ERRORS_BLOB_FOLDER'))
ASSESSMENT_METADATA_BLOB_FOLDER=str(os.environ.get('ASSESSMENT_METADATA_BLOB_FOLDER'))

STUDENT_DATA_BLOB_FOLDER=str(os.environ.get('STUDENT_DATA_BLOB_FOLDER'))
STUDENT_ERRORS_BLOB_FOLDER=str(os.environ.get('STUDENT_ERRORS_BLOB_FOLDER'))


