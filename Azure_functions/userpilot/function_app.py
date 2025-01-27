import azure.functions as func
import logging
from azure.storage.blob import BlobClient
import os
import json
from datetime import datetime  # Already imported, no need to use `datetime.datetime`
import pandas as pd
from azure.storage.blob import BlobClient, BlobType



def write_to_csv_file(connection: str, container: str, filename: str, content: bytes):
    blob = BlobClient.from_connection_string(
        conn_str=connection,
        container_name=container,
        blob_name=filename,
        blob_type=BlobType.AppendBlob
    )

    if not blob.exists():
        blob.create_append_blob()
        blob.append_block(data=content)
    else:
        content_no_headers = '\n'.join(content.split('\n')[1:])
        blob.append_block(data=content_no_headers.encode())



app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="userpilot_webhook")
def userpilot_webhook(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    timestamp = datetime.now().strftime("%Y%m%d")  # e.g., "20240919_120101"
    filename = f"userpilot_data_{timestamp}.csv"


    try:
        req_body = req.get_json()
        logging.info(f'Request Body: {req_body}')



        json_str = json.dumps(req_body, indent=4)
        logging.info(f"json_data: {json_str}")

        data_type = req_body.get('data', {}).get('type', None)  
        logging.info(f'Data Type: {data_type}')

        data = req_body.get('data', {})
        user = data.get('user', {})


        # Handle the Unix timestamp from the input JSON
        timestamp_date = req_body.get('timestamp', None)
        logging.info(f"Raw Unix timestamp from request: {timestamp_date}")

        if timestamp_date is not None:
            try:
                # Directly convert Unix timestamp (in seconds) to datetime
                timestamp_date = datetime.fromtimestamp(timestamp_date)
                timestamp_date_str = timestamp_date.strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError) as e:
                logging.error(f"Error converting timestamp: {e}")
                timestamp_date_str = None
        else:
            timestamp_date_str = None


        # logging.info(f'Filtered Data: {filtered_data}')

        filtered_data = {
        'user_id': user.get('user_id',None),
        'name': user.get('name',None),
        'email': user.get('email',None),
        'role_name': user.get('role_name',None),
        'score': data.get('score',None),
        'feedback': data.get('feedback',None),
        'sessions': user.get('sessions',None),
        'schoolId': user.get('school_id',None),
        'school_name': user.get('school_name',None),
        'district_name': user.get('district_name',None),
        'hostname': data.get('hostname',None),
        'timestamp_date': timestamp_date_str
        }

        df = pd.json_normalize(filtered_data)
        csv_data = df.to_csv(index=False)

        logging.info(f"CSV Data: {csv_data}")

        if data_type == "nps":
            write_to_csv_file(
                os.getenv("connection_str"), 
                "nps-data",                         
                filename,
                csv_data
            )
            
            response_message = "NPS type"
        else:
            write_to_csv_file(
                os.getenv("connection_str"), 
                "non-nps-data",                         
                filename,
                csv_data
            )
            response_message = "Non-NPS type."

        status_code = 200

    except ValueError as e:
        logging.error(f"Error parsing request body: {e}")
        status_code = 400
        response_message = "Invalid request body"

        return func.HttpResponse(
            f"{response_message} (Status Code: {status_code})",
            status_code=status_code
        )
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        status_code = 500
        response_message = "Internal Server Error"

        return func.HttpResponse(
            f"exception: {e} (Status Code: {status_code})",
            status_code=status_code
        )
    
    return func.HttpResponse(
            f"(Status Code: {status_code})",
            status_code=status_code
        )




def write_to_json(connection: str, container: str, filename: str, content: bytes):
    blob = BlobClient.from_connection_string(
        conn_str=connection,
        container_name=container,
        blob_name=filename,
    )
    if not blob.exists():
        blob.upload_blob(data=content)



@app.route(route="userpilot_json", auth_level=func.AuthLevel.FUNCTION)
def userpilot_json(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # e.g., "20240919_120101"
    filename = f"sample_data_{timestamp}.json"

    try:
        req_body = req.get_json()
        logging.info(f'Request Body: {req_body}')
        json_str = json.dumps(req_body, indent=4)
        # pd.read_json()
        logging.info(f"json_data: {json_str}")
        write_to_json(
                os.getenv("connection_str"),
                "json-response",
                filename,
                json_str,
            )

        # Store the req_body to a file/database
        status_code = 200
        response_message = "This HTTP triggered function executed successfully."

    except ValueError as e:
        logging.error(f"Error parsing request body: {e}")
        status_code = 400
        response_message = "Invalid request body"

        return func.HttpResponse(
            f"{response_message} (Status Code: {status_code})",
            status_code=status_code
        )
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        status_code = 500
        response_message = "Internal Server Error"

        return func.HttpResponse(
            f"exception: {e} (Status Code: {status_code})",
            status_code=status_code
        )
    
    return func.HttpResponse(
            f"(Status Code: {status_code})",
            status_code=status_code
        )