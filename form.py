import gspread
import pandas as pd
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
import warnings
warnings.filterwarnings("ignore")
import os


# home_directory = 'C:\Users\Cars24\Desktop\cars24\sahil_creds.json'
gsheet_auth = 'sahil_creds.json'


# gsheet_auth=os.environ.get('SAHIL_SA')
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)

ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1hUMmmrQIXDy2GR9TxfYjYdYLu9jpzTRSq52Hl24sitc/edit#gid=1892535976').worksheet('Sahil')
# gd.set_with_dataframe(ws1,df2,resize=True,row=1,col=1)  #write
fr=pd.DataFrame(ws.get_all_records())  
fr=fr[['Key','Reason for the Re-schedule:','Remarks (captured by driver):']]
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1CUWKMUGl5ninrWOzKGob1y3h3lxiC_EgGCw2N9qVsZw/edit#gid=0').worksheet('G_form_response1')
gd.set_with_dataframe(ws,fr,resize=True,row=1,col=1)  #write
#fr=pd.DataFrame(ws.get_all_records())  
