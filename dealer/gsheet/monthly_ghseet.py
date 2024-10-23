import gspread
import pandas as pd
import gspread_dataframe as gd
import datetime
import os
import time
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import datetime as dt
import numpy as np
import warnings
import sys
import gspread
import warnings
import sys
import imgkit
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
warnings.filterwarnings("ignore")
import gspread
import gspread_dataframe as gd
gsheet_auth = 
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)
import datetime
today = datetime.datetime.now()
today_date = today.strftime('%b')
print(today)
print(today_date)


def chunked(iterable, size):
    return [iterable[i:i+size] for i in range(0, len(iterable), size)]

#Form Response

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

time.sleep(10)


# #Simpler Data (Control Base Tower)

df1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1tZL5V-w8gu6SMRCc6Hzm0IkkOfVXjjZ55ltaMdngOtg/edit?gid=328438549#gid=328438549').worksheet('Processed_Data')
df1 = pd.DataFrame(df1.get_all_records())
df1=df1[['LEAD_ID','LOAN_APPLICATION_ID','CONTACT_NUMBER','LATEST_LEAD_CREATION_TIMESTAMP','LOGIN_DATE','FINAL_TENURE','FINAL_ROI','FINAL_DISBURSABLE_LOAN_AMOUNT','FINAL_TOTAL_LOAN_AMOUNT','CREDIT_APPROVAL_FLAG','CHANNEL_DS','DC_FTR','TOTAL_LTV','CREDIT_APPROVED_TIMESTAMP','TNC_ACCEPTED_TIMESTAMP','LEAD_MODIFIED_BY','DEALER_CITY','DEALER_CODE2','CASE_STATUS','DISBURSED_TIME','HPA_STATUS','LAST_RISK_BUCKET','Final Dealer code','DEALER_CITY','Final FOS','Credit LTV','DS_ROI','DS_CHANNEL']]
df1 = df1[~df1['LEAD_ID'].isna()]
print(df1)
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/144xWGvX7ipabfIkQUvIdzZY_wbwDLwzfjVLyoUmLvDA/edit?gid=1271618088#gid=1271618088').worksheet('Simpler_data_raw')
ws.batch_clear(['A1:AB'])
gd.set_with_dataframe(ws,df1,resize=False,row=1,col=1)  