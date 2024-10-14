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
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1du4ATpQR3unCbPTVjpuZsnyD-lvmWN5t2S_Vy3-tuYw/edit#gid=882714774').worksheet('Raw Data')
# gd.set_with_dataframe(ws2,df,resize=True,row=1,col=1)  
dff=pd.DataFrame(ws.get_all_records())
df=dff.replace('',np.nan)
df = df[~df['LEAD_ID'].isna()]
df=df[df['CHD Remarks 1'].isin(['G form not filled'])]
df['updated_at']=today
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1QRs5O70_siBuDgCM4OWVB5w02vhTf4XDrdMzwjJqKFA/edit#gid=0').worksheet('CHD')
gd.set_with_dataframe(ws,df,resize=True,row=1,col=1)  
# df=pd.DataFrame(ws.get_all_records())
dfr=dff.replace('',np.nan)
# dfr=dfr[dfr['Month']==today_date]
dfr = dfr[~dfr['CHD Remarks 1'].isna()]
print(dfr)
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1qfcojWdvI8kqCnTwJmkZJC1-XsUX6R_LwHT3KT0I5ng/edit#gid=0').worksheet('CHD')
gd.set_with_dataframe(ws,dfr,resize=True,row=1,col=1)  
##eVTF
df1=dff[dff['EVTF Remarks 1'].isin(['G form not filled'])]
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1QRs5O70_siBuDgCM4OWVB5w02vhTf4XDrdMzwjJqKFA/edit#gid=0').worksheet('eVTF')
gd.set_with_dataframe(ws,df1,resize=True,row=1,col=1)  
# df=pd.DataFrame(ws.get_all_records())
dfe=dff.replace('',np.nan) 
# dfe=dfe[dfe['Month']==today_date]
dfe = dfe[~dfe['EVTF Remarks 1'].isna()]

ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1qfcojWdvI8kqCnTwJmkZJC1-XsUX6R_LwHT3KT0I5ng/edit#gid=0').worksheet('eVTF')
gd.set_with_dataframe(ws,dfe,resize=True,row=1,col=1)  
##############################################SLOT Adhernece##################################
ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1CUWKMUGl5ninrWOzKGob1y3h3lxiC_EgGCw2N9qVsZw/edit#gid=0').worksheet('Raw Data')
# gd.set_with_dataframe(ws2,df,resize=True,row=1,col=1)  
df22=pd.DataFrame(ws1.get_all_records())
df2=df22.replace('',np.nan)
df2 = df2[~df2['LEAD_ID'].isna()]
df2=df2[df2['Regional Team Remarks 2'].isin(['G-form not Filled'])]
ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1QRs5O70_siBuDgCM4OWVB5w02vhTf4XDrdMzwjJqKFA/edit#gid=0').worksheet('Slot_Adherence')
gd.set_with_dataframe(ws1,df2,resize=True,row=1,col=1)  
# df=pd.DataFrame(ws1.get_all_records())
dfs=df22.replace('',np.nan)
# dfs=dfs[dfs['Req_Month']==today_date]
dfs=dfs[~dfs['Regional Team Remarks 2'].isna()]
ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1qfcojWdvI8kqCnTwJmkZJC1-XsUX6R_LwHT3KT0I5ng/edit#gid=362389993').worksheet('Slot_Adherence')
gd.set_with_dataframe(ws1,dfs,resize=True,row=1,col=1)  
########################Driver_QC#####################################
ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1xuboT__o4o7sIA53-MrVUbXERMFRv2M2AY1q9PYS9YM/edit#gid=881800330').worksheet('Raw_data')
# gd.set_with_dataframe(ws2,df,resize=True,row=1,col=1)  
df33=pd.DataFrame(ws1.get_all_records())
df3=df33.replace('',np.nan)
df3 = df3[~df3['LEAD_ID'].isna()]
df3=df3[df3['Logistics RCA 1'].isin(['G-Form Not Filled'])]
ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1QRs5O70_siBuDgCM4OWVB5w02vhTf4XDrdMzwjJqKFA/edit#gid=0').worksheet('Driver_QC')
gd.set_with_dataframe(ws1,df3,resize=True,row=1,col=1)  
# df=pd.DataFrame(ws1.get_all_records())
dq=df33.replace('',np.nan)
# dq=dq[dq['Month']==today_date]
dq=dq[dq['Is Eligible for QC']=='Eligible']
# dq['Logistics RCA 1']=dq['Logistics RCA 1'].fillna('',np.nan)
dq=dq[~dq['Logistics RCA 1'].isna()]
ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1qfcojWdvI8kqCnTwJmkZJC1-XsUX6R_LwHT3KT0I5ng/edit#gid=362389993').worksheet('Driver_QC')
gd.set_with_dataframe(ws1,dq,resize=True,row=1,col=1)  
