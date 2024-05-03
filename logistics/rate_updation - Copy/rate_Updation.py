import snowflake.connector
import gspread
import pandas as pd
import gspread_dataframe as gd
import time
import winsound
import os
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import gdown
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import psycopg2 as pg
import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
import datetime as dt
import time as tm
import numpy as np
import warnings
warnings.filterwarnings("ignore")
import numpy as npx
import gspread
import gspread_dataframe as gd
home_directory = 'C:/Users/Cars24/Desktop/Notebook/'
gsheet_auth = f'{home_directory}sahil_creds.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)

###CHD
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1du4ATpQR3unCbPTVjpuZsnyD-lvmWN5t2S_Vy3-tuYw/edit#gid=882714774').worksheet('Raw Data')
# gd.set_with_dataframe(ws2,df,resize=True,row=1,col=1)  
dff=pd.DataFrame(ws.get_all_records())
df=dff.replace('',np.nan)
df = df[~df['LEAD_ID'].isna()]
df=df[df['CHD Remarks 1'].isin(['G form not filled'])]
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1QRs5O70_siBuDgCM4OWVB5w02vhTf4XDrdMzwjJqKFA/edit#gid=0').worksheet('CHD')
gd.set_with_dataframe(ws,df,resize=True,row=1,col=1)  
# df=pd.DataFrame(ws.get_all_records())

##eVTF
df1=dff[dff['EVTF Remarks 1'].isin(['G form not filled'])]
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1QRs5O70_siBuDgCM4OWVB5w02vhTf4XDrdMzwjJqKFA/edit#gid=0').worksheet('eVTF')
gd.set_with_dataframe(ws,df1,resize=True,row=1,col=1)  
# df=pd.DataFrame(ws.get_all_records())

ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1CUWKMUGl5ninrWOzKGob1y3h3lxiC_EgGCw2N9qVsZw/edit#gid=0').worksheet('Raw Data')
# gd.set_with_dataframe(ws2,df,resize=True,row=1,col=1)  
df2=pd.DataFrame(ws1.get_all_records())
df2=df2.replace('',np.nan)
df2 = df2[~df2['LEAD_ID'].isna()]
df2=df2[df2['Regional Team Remarks 2'].isin(['G-form not Filled'])]
ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1QRs5O70_siBuDgCM4OWVB5w02vhTf4XDrdMzwjJqKFA/edit#gid=0').worksheet('Slot_Adherence')
gd.set_with_dataframe(ws1,df2,resize=True,row=1,col=1)  
# df=pd.DataFrame(ws1.get_all_records())




ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1xuboT__o4o7sIA53-MrVUbXERMFRv2M2AY1q9PYS9YM/edit#gid=881800330').worksheet('Raw_data')
# gd.set_with_dataframe(ws2,df,resize=True,row=1,col=1)  
df3=pd.DataFrame(ws1.get_all_records())
df3=df3.replace('',np.nan)
df3 = df3[~df3['LEAD_ID'].isna()]
df3=df3[df3['Logistics RCA 1'].isin(['G-Form Not Filled'])]
ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1QRs5O70_siBuDgCM4OWVB5w02vhTf4XDrdMzwjJqKFA/edit#gid=0').worksheet('Driver_QC')
gd.set_with_dataframe(ws1,df3,resize=True,row=1,col=1)  
# df=pd.DataFrame(ws1.get_all_records())







