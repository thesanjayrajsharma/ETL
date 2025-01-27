from src.load import *
from src.etl import run_student_assessment_pull
from src.env_config import STUDENT_TEMP_PATH
from src.config import data_type


if __name__ == "__main__":

    run_student_assessment_pull()
    truncate_assessment_staging()
    lazy_load_df_into_db(
                file_path='',
                table_name='FEV_NWEA_ASSESSMENT_STAGE',
                schema='DWH_STAGING',
                chunksize=10000,
                db_dtype=data_type,
                if_exists='append',
                     )

      
