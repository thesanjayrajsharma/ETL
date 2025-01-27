import gspread
from google.oauth2.service_account import Credentials
import pandas as pd


SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file('/home/fevdevadmin/etl_repo/ETL/scripts/adhoc/NSSA_QC/config.json', scopes=SCOPES)


client = gspread.authorize(creds)


spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1i8UfNZxgEs8sDkrYaWSoIx_vZ2QoGl3cpT1AnEX3S_U/edit?resourcekey=&gid=15268086#gid=15268086')

worksheet = spreadsheet.get_worksheet_by_id(15268086)

# Read all values from the sheet
data = worksheet.get_all_values()


df = pd.DataFrame(data)

# Skip the first row (header) from Google Sheets data
df_without_header = df.iloc[1:]  # This removes the first row (index 0)


csv_file_path = r'/home/fevdevadmin/etl_repo/ETL/scripts/adhoc/NSSA_QC/TL_feedback_data.csv'

df_without_header.to_csv(csv_file_path, mode='a', index=False, header=False)

# Delete data from Google Sheets (keeping the headers)
if not df_without_header.empty:
    # Get the number of rows to delete
    num_rows = len(df_without_header)
    
    # Delete the rows starting from row 2 (index 1) to the end
    worksheet.delete_rows(2, num_rows + 1)

print(f"Data successfully appended to {csv_file_path} and deleted from Google Sheets.")
