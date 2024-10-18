import gspread
import pandas as pd
import gspread_dataframe as gd
import datetime
import os
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import pandas as pd
from datetime import datetime
import datetime as dt
import numpy as np
import warnings
import sys
warnings.filterwarnings("ignore")
import numpy as np
import gspread
import gspread_dataframe as gd
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
source_ws = gc.open_by_url('https://docs.google.com/spreadsheets/d/1tZL5V-w8gu6SMRCc6Hzm0IkkOfVXjjZ55ltaMdngOtg/edit?gid=328438549#gid=328438549').worksheet('Monthly_sheet')
dff = pd.DataFrame(source_ws.get_all_records())
print(dff)
df=dff[['Total Loan Sanction','Loan ID','Loan for CHM','Dealer Code','ROI per annum','Timestamp1','HPA Status','Final Status','Loan Tenure','City','PF %','Loan for MI','MI Holdback']]
df=dff.replace('',np.nan)
df = df[~df['Loan ID'].isna()]
print(df)
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/144xWGvX7ipabfIkQUvIdzZY_wbwDLwzfjVLyoUmLvDA/edit?gid=1960642873#gid=1960642873').worksheet('Form Responses 4')
ws.batch_clear(['A1:M'])
gd.set_with_dataframe(ws,df,resize=False,row=1,col=1)  

