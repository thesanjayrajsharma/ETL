from prefect import flow, task
from src.etl import run_assessment_data_pull,run_student_data_pull
from src.assessment_load import *
from src.student_load import *
from src.config import delete_old_files, send_email,setup_logging
from src.azure_upload import *

@flow
def assessment_data_flow():
    """Main function to run the complete ETL"""
    try:
        run_assessment_data_pull()
        run_student_data_pull(src=get_student_bids())
        truncate_student_staging()  
        truncate_assessment_staging()      
        load_student_data_to_staging()
        run_student_d_procedure() 
        load_assessment_data_to_staging()
        run_assessment_d_procedure()
        run_assessment_f_procedure()
        upload_files_to_azure_storage()
        delete_old_files()
    except Exception as e:
        send_email(
            subject=f"{REGION} NWEA Assessment ETL failure",
            msg=f"An error occurred during the {REGION} NWEA Assessment ETL run: {str(e)}",
            email_to=RECEIVERS_EMAIL
        )    



if __name__ == '__main__':
    setup_logging()
    assessment_data_flow()
  




 


