from contextlib import contextmanager
import logging
import oracledb
from  .env_config import (
    ORACLE_DB_PASSWORD,
    ORACLE_DB_TNS,
    ORACLE_DB_USERNAME,
)
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import cx_Oracle
import pandas as pd
oracle_pool = None


# def init_oracle_db():
#     try:
#         oracledb.init_oracle_client(lib_dir=ORACLE_DRIVER_PATH)
#     except Exception as e:
#         print(e)
@contextmanager
def __get_oracle_connection():
    # con = oracledb.connect(
    #     user=ORACLE_DB_USERNAME,
    #     password=ORACLE_DB_PASSWORD,
    #     dsn=ORACLE_DB_TNS,
    # )
    global oracle_pool
    if not oracle_pool:
        oracle_pool = oracledb.create_pool(
            user=ORACLE_DB_USERNAME,
            password=ORACLE_DB_PASSWORD,
            dsn=ORACLE_DB_TNS,
            min=20,             #10
            max=100,             #50
            increment=5,        #1
            getmode=oracledb.POOL_GETMODE_NOWAIT,
        )
    con = oracle_pool.acquire()
    try:
        yield con
    except Exception as e:
        print(e, "Error while creating connection")
    finally:
        con.commit()
        con.close()



def execute_query_to_df(sql: str):
    engine = create_engine(
    f"oracle://{ORACLE_DB_USERNAME}:{ORACLE_DB_PASSWORD}@{ORACLE_DB_TNS}/?encoding=UTF-8&nencoding=UTF-8",
    max_identifier_length=128,pool_size=10, max_overflow=20
)
    df = pd.read_sql_query(sql, engine)
    engine.dispose()
    return df

def execute_oracle_query(sql: str):
    with __get_oracle_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]


def get_oracle_connection_sqlalchemy():
    global oracle_pool
    if not oracle_pool:
        oracle_pool = cx_Oracle.SessionPool(
            user=ORACLE_DB_USERNAME,
            password=ORACLE_DB_PASSWORD,
            dsn=ORACLE_DB_TNS,
            min=10,
            max=50,
            increment=1,
            encoding="UTF-8",
            nencoding="UTF-8",
            threaded=True
        )
    engine = create_engine(
        "oracle://",
        creator=oracle_pool.acquire,
        poolclass=NullPool,
        max_identifier_length=128
    )
    return engine


def insert_dataframe(
    table_name: str,
    df: DataFrame,
    *,
    schema=None,
    if_exists="fail",
    index=False,
    index_label=None,
    chunksize=None,
    dtype=None,
   # method="multi",
):
    engine = create_engine(
    f"oracle://{ORACLE_DB_USERNAME}:{ORACLE_DB_PASSWORD}@{ORACLE_DB_TNS}/?encoding=UTF-8&nencoding=UTF-8",
    max_identifier_length=128,
)

    print(f"Value of if_exists: {if_exists}")
    with engine.connect() as connection:
        df.to_sql(
            table_name,
            connection,
            schema=schema,
            if_exists=if_exists,
            index=index,
            index_label=index_label,
            chunksize=chunksize,
            dtype=dtype,
           # method=method,
        )

def lazy_load_df(
    file_path: str, table_name: str, schema: str, chunksize: int = 10000, dtype=None
) -> None:
    """
    Lazily load in a dataframe and pass in the chunks to a dataframe
    Arguments:
    file_path, str:  path of the file to read in
    table_name, str: name of the table to insert data into
    schema:  database schema to use for insert
    chunsize, int:  number of lines to read into the file
    Returns:
    None
    """
    for idx, file_chunk in enumerate(
        pd.read_csv(file_path, chunksize=chunksize, dtype=dtype)
    ):
        if idx % 10 == 0:
            logging.info(f"On round {idx}")
        try:
            insert_dataframe(table_name, file_chunk, schema=schema, chunksize=chunksize)
        except Exception as e:
            logging.error(f"Could not load data for round {idx} because: {e}")
            raise





'''call oracle procedures'''

def prepare_data_in_data_warehouse(procedure_name, param):
    engine = get_oracle_connection_sqlalchemy()
    connection = engine.raw_connection()
    cursor = connection.cursor()  # Create cursor

    try:
        x_load_status = cursor.var(str)  # Assuming VARCHAR2 is mapped to str

        # Call the stored procedure
        cursor.callproc(procedure_name, [x_load_status])  
        connection.commit()  
        
        # Get the value of the OUT parameter (x_load_Status) of the procedure
        status = x_load_status.getvalue()

        if status == 'E':
            raise Exception("Stored procedure failed with status:'E'.")

        logging.info("Procedure called successfully. Status: %s", status)
        
    except Exception as e:
        logging.error("Error calling procedure: %s", e)
        connection.rollback()  # Rollback in case of error
        raise  # Re-raise the exception to propagate failure

    finally:
        cursor.close()  
        connection.close()  # Close the connection




      