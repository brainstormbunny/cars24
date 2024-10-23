import sys
import pandas as pd
import gspread_dataframe as gd
import os
from oauth2client.service_account import ServiceAccountCredentials
import warnings
warnings.filterwarnings("ignore")
import numpy as np
import gspread
gsheet_auth = 'sahil_creds.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)
import datetime
today = datetime.datetime.now()
today_date = today.strftime('%b')
print(today)
print(today_date)
###CHD
source_ws = gc.open_by_url('https://docs.google.com/spreadsheets/d/1tZL5V-w8gu6SMRCc6Hzm0IkkOfVXjjZ55ltaMdngOtg/edit?gid=0#gid=0').worksheet('Form responses 4')
dff = pd.DataFrame(source_ws.get_all_records())
print(dff)
df=dff[['Loan ID',	'Total Loan Sanction',	'Documentation Charges',	'Stamp Duty Charges',	'Valuation Charges',	'Cibil Charges',	' PF Amount']]
df=df.replace('',np.nan)
df = df[~df['Loan ID'].isna()]
print(df)
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/144xWGvX7ipabfIkQUvIdzZY_wbwDLwzfjVLyoUmLvDA/edit?pli=1&gid=1463719920#gid=1463719920').worksheet('Form Responses 4_1')
ws.batch_clear(['A1:G'])
gd.set_with_dataframe(ws,df,resize=False,row=1,col=1)
