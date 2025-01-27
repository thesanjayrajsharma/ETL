import os
from dotenv import load_dotenv


'''path to .env remains same across prod and dev'''
dotenv_path = '/home/fevdevadmin/etl_repo/ETL/scripts/classlink/src/.env'


load_dotenv(dotenv_path)

CLASSLINK_BEARER =str(os.environ.get('CLASSLINK_BEARER'))
CLASSLINK_ALL_STUDENTS=str(os.environ.get('CLASSLINK_ALL_STUDENTS'))
CLASSLINK_API_STUDENT_IDS=str(os.environ.get('CLASSLINK_API_STUDENT_IDS'))
CLASSLINK_BULK_DATA=str(os.environ.get('CLASSLINK_BULK_DATA'))
CLASSLINK_ERROR_LOGS=str(os.environ.get('CLASSLINK_ERROR_LOGS'))
CLASSLINK_INCREMENTAL_DATA=str(os.environ.get('CLASSLINK_INCREMENTAL_DATA'))
ROOT_DIR=str(os.environ.get('ROOT_DIR'))
REGION=str(os.environ.get('REGION'))
RECEIVERS_EMAIL=str(os.environ.get('RECEIVERS_EMAIL'))
CONNECTION_STRING=str(os.environ.get('CONNECTION_STRING'))
CONTAINER=str(os.environ.get('CONTAINER'))



