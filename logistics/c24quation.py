import gspread
import pandas as pd
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
import warnings
warnings.filterwarnings("ignore")
import os
import sys
import snowflake.connector
import numpy as np

email = 'sahil.5@cars24.com'
WAREHOUSE = 'BI_WH'

# Connect to Snowflake
ctx = snowflake.connector.connect(
    user=email,
    warehouse=WAREHOUSE,
    account='am62076.ap-southeast-2',
    authenticator="externalbrowser",  # Use external browser authentication
    private_key_path='key.pub',  # Path to your private key
)

cur = ctx.cursor()

# home_directory = 'C:\Users\Cars24\Desktop\cars24\sahil_creds.json'
gsheet_auth = 'sahil_creds.json'


# gsheet_auth=os.environ.get('SAHIL_SA')
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)


def chunked(iterable, size):
    return [iterable[i:i+size] for i in range(0, len(iterable), size)]


sheet_url = 'https://docs.google.com/spreadsheets/d/14ANHPawQlKMPfWXgZJW_75EZdoYREWdLEJum9oPzeq4/edit#gid=0'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("Sheet1")
cell_range1 = worksheet.range("A1:A")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 1)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace('',np.nan)
data1=data1[~data1['LEAD_ID'].isna()]
print(data1)


data1['LEAD_ID'] = data1['LEAD_ID'].astype(str)

print(data1)


# Create a tuple of lead IDs
lead_id_tuple = tuple(data1['LEAD_ID'])
print(lead_id_tuple)


query = """
Select S.* from PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS AS ORDERS

LEFT JOIN (Select APPT_ID,C24_QUOTE from "PC_STITCH_DB"."FIVETRAN1_BI"."SALES_TRANSACTIONS") S ON ORDERS.LEAD_ID = S.APPT_ID

WHERE LEAD_ID IN """+str(lead_id_tuple)+""";
        """  

cur.execute(query)
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/14ANHPawQlKMPfWXgZJW_75EZdoYREWdLEJum9oPzeq4/edit#gid=0').worksheet('Sheet2')
gd.set_with_dataframe(ws,df,resize=False,row=1,col=1)  #write
#