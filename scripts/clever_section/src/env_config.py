import os
from dotenv import load_dotenv

'''manually add the path as Prefect does not locate the .env file'''
dotenv_path='/home/fevdevadmin/etl_repo/ETL/scripts/clever_section/src/.env'



load_dotenv(dotenv_path)

CLEVER_BEARER =str(os.environ.get('CLEVER_BEARER'))
CLEVER_BULK_DATA=str(os.environ.get('CLEVER_SECTION_BULK_DATA'))
CLEVER_ERROR_LOGS=str(os.environ.get('CLEVER_SECTION_ERROR_LOGS'))
CLEVER_INCREMENTAL_DATA=str(os.environ.get('CLEVER_SECTION_INCREMENTAL_DATA'))
ROOT_DIR=str(os.environ.get('ROOT_DIR'))
REGION=str(os.environ.get('REGION'))
RECEIVERS_EMAIL=str(os.environ.get('RECEIVERS_EMAIL'))
CONNECTION_STRING=str(os.environ.get('CONNECTION_STRING'))
CONTAINER=str(os.environ.get('CONTAINER'))