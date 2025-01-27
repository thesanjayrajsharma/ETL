import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
ORACLE_DB_USERNAME = str(os.environ.get("ORACLE_DB_USERNAME"))
ORACLE_DB_PASSWORD = str(os.environ.get("ORACLE_DB_PASSWORD"))
ORACLE_DB_TNS = str(os.environ.get("ORACLE_DB_TNS"))
CLASSLINK_BEARER=str(os.environ.get("CLASSLINK_BEARER"))
ORACLE_DRIVER_PATH = str(os.environ.get("ORACLE_DRIVER_PATH"))
