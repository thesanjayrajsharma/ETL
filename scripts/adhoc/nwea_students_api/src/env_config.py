import os
from dotenv import load_dotenv

dotenv_path = '/home/fevdevadmin/etl_repo/ETL/scripts/adhoc/nwea_students_api/src/.env'

load_dotenv(dotenv_path)

NWEA_AUTH = str(os.environ.get('NWEA_AUTH'))
NWEA_API_KEY = str(os.environ.get('NWEA_API_KEY'))
ROOT_DIR=str(os.environ.get('ROOT_DIR'))
STUDENT_TEMP_PATH = str(os.environ.get('STUDENT_TEMP_PATH'))
STUDENT_ERRORS_PATH = str(os.environ.get('STUDENT_ERRORS_PATH'))
STUDENT_NODATA_PATH = str(os.environ.get('STUDENT_NODATA_PATH'))
STUDENT_DATA_PATH = str(os.environ.get('STUDENT_DATA_PATH'))

