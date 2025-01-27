import os
from dotenv import load_dotenv

'''manually add the path as Prefect does not locate the .env file'''
dotenv_path='/home/fevdevadmin/etl_repo/ETL/scripts/userpilot/src/.env'


load_dotenv(dotenv_path)
ROOT_DIR=str(os.environ.get('ROOT_DIR'))

connection_string = str(os.environ.get('connection_string'))
container_name = str(os.environ.get('container_name'))