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
gsheet_auth = 'sahil_creds.json'
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
########################################################################################################################

#Monthly_Updated_Sheet - Form_Response4

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

time.sleep(2)

########################################################################################################################
#Simpler_Data_from_(Control Base Tower)_TO_Monthly_Updated_sheet

df1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1tZL5V-w8gu6SMRCc6Hzm0IkkOfVXjjZ55ltaMdngOtg/edit?gid=328438549#gid=328438549').worksheet('Processed_Data')
df1 = pd.DataFrame(df1.get_all_records())
df1=df1[['LEAD_ID','LOAN_APPLICATION_ID','CONTACT_NUMBER','LATEST_LEAD_CREATION_TIMESTAMP','LOGIN_DATE','FINAL_TENURE','FINAL_ROI','FINAL_DISBURSABLE_LOAN_AMOUNT','FINAL_TOTAL_LOAN_AMOUNT','CREDIT_APPROVAL_FLAG','CHANNEL_DS','DC_FTR','TOTAL_LTV','CREDIT_APPROVED_TIMESTAMP','TNC_ACCEPTED_TIMESTAMP','LEAD_MODIFIED_BY','DEALER_CITY','DEALER_CODE2','CASE_STATUS','DISBURSED_TIME','HPA_STATUS','LAST_RISK_BUCKET','Final Dealer code','DEALER_CITY','Final FOS','Credit LTV','DS_ROI','DS_CHANNEL']]
df1 = df1[~df1['LEAD_ID'].isna()]
print(df1)
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/144xWGvX7ipabfIkQUvIdzZY_wbwDLwzfjVLyoUmLvDA/edit?gid=1271618088#gid=1271618088').worksheet('Simpler_data_raw')
ws.batch_clear(['A1:AB'])
gd.set_with_dataframe(ws,df1,resize=False,row=1,col=1)  

time.sleep(2)
########################################################################################################################
# Contest_sheet_(SIMPLER DATA)

def chunked(iterable, size):
    return [iterable[i:i+size] for i in range(0, len(iterable), size)]
sheet_url1 = 'https://docs.google.com/spreadsheets/d/144xWGvX7ipabfIkQUvIdzZY_wbwDLwzfjVLyoUmLvDA/edit?gid=0#gid=0'
sheet = gc.open_by_url(sheet_url1)
worksheet = sheet.worksheet("Simpler_data")
cell_range11 = worksheet.range("A1:AE")
data = [[cell.value for cell in row] for row in chunked(cell_range11, 31)]
data11 = pd.DataFrame(data)
data11.columns = data11.iloc[0]
data11 = data11.drop(data11.index[0]).reset_index(drop=True)
data11=data11.replace(np.nan,'')


ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1KYSg23PXx0UlcPLEva545GU_41y3flsEmoD5oj2mwlw/edit?pli=1&gid=0#gid=0').worksheet('Simpler_data')
ws.batch_clear(['A1:AE'])
gd.set_with_dataframe(ws,data11,resize=False,row=1,col=1)  

########################################################################################################################
# Contest_sheet_(Form Response4)

sheet_url11 = 'https://docs.google.com/spreadsheets/d/144xWGvX7ipabfIkQUvIdzZY_wbwDLwzfjVLyoUmLvDA/edit?gid=1960642873#gid=1960642873'
sheet = gc.open_by_url(sheet_url11)
worksheet = sheet.worksheet("Form Responses 4")
cell_range11 = worksheet.range("A1:AJ")
data = [[cell.value for cell in row] for row in chunked(cell_range11, 36)]
data11 = pd.DataFrame(data)
data11.columns = data11.iloc[0]
data11 = data11.drop(data11.index[0]).reset_index(drop=True)
data11=data11.replace(np.nan,'')
data11

ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1KYSg23PXx0UlcPLEva545GU_41y3flsEmoD5oj2mwlw/edit?pli=1&gid=1960642873#gid=1960642873').worksheet('Form Responses 4')
ws.batch_clear(['A1:AJ'])
gd.set_with_dataframe(ws,data11,resize=False,row=1,col=1)  
sys.exit()

###############################################################
sheet_url = 'https://docs.google.com/spreadsheets/d/144xWGvX7ipabfIkQUvIdzZY_wbwDLwzfjVLyoUmLvDA/edit?gid=599981802#gid=599981802'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("Report_SM/RSM")
cell_range1 = worksheet.range("D3:K6")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 8)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace(np.nan,'')



html_table = data1.to_html(escape=False, index=False)

html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{
            width: fit-content;
            font-family: 'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif;
            align-content: center;
        }}

        table, th, td {{
            border: 1px solid rgb(5, 9, 30);
            border-collapse: collapse;
            text-align: center;
            text-indent: 5px;
        }}

        td {{
            max-height: fit-content;
            max-width: fit-content;
        }}

        #firstdiv {{
            border-top: 2px solid rgb(76, 104, 65);
            border-left: 2px solid rgb(76, 104, 65);
            border-right: 2px solid rgb(76, 104, 65);
            background-color: rgb(50, 91, 168);
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            padding-bottom: 2px;
            color: white;
        }}

        h3 {{
            font-size: 35px;
            margin-top: 10px;
        }}

        h4 {{
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            text-align: left;
            padding-left: 10px;
            color: #ffffff;
            background-color: rgb(5, 9, 30);
            border: 2px solid rgb(5, 9, 30);
        }}

        th {{
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            color: #ffffff;
        }}
    </style>
</head>
<body style="display: inline-flexbox; align-content: center;">
    <div id="firstdiv" style="text-align: center; background-color: black; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: auto;">
        <h3>Disbursment (RSM-Wise) </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Disburment (October) :</h4>
        {html_table}
    </div>
   
</body>
</html>
"""

html_file_path = 'report.html'

# Save the HTML content to a file
with open(html_file_path, 'w') as file:
    file.write(html_template)

print(f"HTML file saved successfully at: {html_file_path}")


def html_to_png(html_file, output_file):
    # Configure headless Chrome
    options = Options()
    options.add_argument("--headless")  # Run in headless mode, i.e., without opening a browser window
    driver = webdriver.Chrome(options=options)
    
    driver.get("file:///" + os.path.abspath(html_file))
    
    time.sleep(2) 
    
    driver.set_window_size(750, 450)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'report.html'
    png_file_path = 'report.png'

    html_to_png(html_file_path, png_file_path)
    
secret_key=os.environ['secret_key']

slack_token = secret_key
client = WebClient(token=slack_token)

channel=['C07V24TDK7T']
for i  in channel:
    image_path = png_file_path
    channel=i
    try:
        response = client.files_upload(
            channels=channel,
            file=image_path,
            title=f'''_Disbursment (RSM-Wise)_
            ''',
            initial_comment=f'''            '''
        )

        if response['ok']:
            print("Image sent successfully!")
        else:
            print("Failed to send image:", response['error'])

    except SlackApiError as e:
        print(f"Error sending image: {e.response['error']}")

time.sleep(2)
cell_range2 = worksheet.range("N3:U6")
data = [[cell.value for cell in row] for row in chunked(cell_range2, 8)]
data2 = pd.DataFrame(data)
data2.columns = data2.iloc[0]
data2 = data2.drop(data1.index[0]).reset_index(drop=True)
data2=data2.replace(np.nan,'')


html_table1 = data2.to_html(escape=False, index=False)

html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{
            width: fit-content;
            font-family: 'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif;
            align-content: center;
        }}

        table, th, td {{
            border: 1px solid rgb(5, 9, 30);
            border-collapse: collapse;
            text-align: center;
            text-indent: 5px;
        }}

        td {{
            max-height: fit-content;
            max-width: fit-content;
        }}

        #firstdiv {{
            border-top: 2px solid rgb(76, 104, 65);
            border-left: 2px solid rgb(76, 104, 65);
            border-right: 2px solid rgb(76, 104, 65);
            background-color: rgb(50, 91, 168);
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            padding-bottom: 2px;
            color: white;
        }}

        h3 {{
            font-size: 35px;
            margin-top: 10px;
        }}

        h4 {{
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            text-align: left;
            padding-left: 10px;
            color: #ffffff;
            background-color: rgb(5, 9, 30);
            border: 2px solid rgb(5, 9, 30);
        }}

        th {{
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            color: #ffffff;
        }}
    </style>
</head>
<body style="display: inline-flexbox; align-content: center;">
    <div id="firstdiv" style="text-align: center; background-color: black; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: auto;">
        <h3>Login Report (RSM-Wise) </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Login Report (October) :</h4>
        {html_table1}
    </div>
   
</body>
</html>
"""

html_file_path = 'report1.html'

# Save the HTML content to a file
with open(html_file_path, 'w') as file:
    file.write(html_template)

print(f"HTML file saved successfully at: {html_file_path}")


def html_to_png(html_file, output_file):
    # Configure headless Chrome
    options = Options()
    options.add_argument("--headless")  # Run in headless mode, i.e., without opening a browser window
    driver = webdriver.Chrome(options=options)
    
    driver.get("file:///" + os.path.abspath(html_file))
    
    time.sleep(2) 
    
    driver.set_window_size(750, 450)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'report1.html'
    png_file_path = 'report1.png'

    html_to_png(html_file_path, png_file_path)
    
secret_key=os.environ['secret_key']

slack_token = secret_key
client = WebClient(token=slack_token)

channel=['C07V24TDK7T']
for i  in channel:
    image_path = png_file_path
    channel=i
    try:
        response = client.files_upload(
            channels=channel,
            file=image_path,
            title=f'''_Login Report (RSM-Wise)_
            ''',
            initial_comment=f'''            '''
        )

        if response['ok']:
            print("Image sent successfully!")
        else:
            print("Failed to send image:", response['error'])

    except SlackApiError as e:
        print(f"Error sending image: {e.response['error']}")

time.sleep(2)
cell_range3 = worksheet.range("C10:K28")
data = [[cell.value for cell in row] for row in chunked(cell_range3, 9)]
data3 = pd.DataFrame(data)
data3.columns = data3.iloc[0]
data3 = data3.drop(data3.index[0]).reset_index(drop=True)
data3=data3.replace(np.nan,'')


html_table3 = data3.to_html(escape=False, index=False)

html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{
            width: fit-content;
            font-family: 'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif;
            align-content: center;
        }}

        table, th, td {{
            border: 1px solid rgb(5, 9, 30);
            border-collapse: collapse;
            text-align: center;
            text-indent: 5px;
        }}

        td {{
            max-height: fit-content;
            max-width: fit-content;
        }}

        #firstdiv {{
            border-top: 2px solid rgb(76, 104, 65);
            border-left: 2px solid rgb(76, 104, 65);
            border-right: 2px solid rgb(76, 104, 65);
            background-color: rgb(50, 91, 168);
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            padding-bottom: 2px;
            color: white;
        }}

        h3 {{
            font-size: 35px;
            margin-top: 10px;
        }}

        h4 {{
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            text-align: left;
            padding-left: 10px;
            color: #ffffff;
            background-color: rgb(5, 9, 30);
            border: 2px solid rgb(5, 9, 30);
        }}

        th {{
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            color: #ffffff;
        }}
    </style>
</head>
<body style="display: inline-flexbox; align-content: center;">
    <div id="firstdiv" style="text-align: center; background-color: black; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: auto;">
        <h3>Disbursment Report (SM-Wise) </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Disbursment Report (October) :</h4>
        {html_table3}
    </div>
   
</body>
</html>
"""

html_file_path = 'report2.html'

# Save the HTML content to a file
with open(html_file_path, 'w') as file:
    file.write(html_template)

print(f"HTML file saved successfully at: {html_file_path}")


def html_to_png(html_file, output_file):
    # Configure headless Chrome
    options = Options()
    options.add_argument("--headless")  # Run in headless mode, i.e., without opening a browser window
    driver = webdriver.Chrome(options=options)
    
    driver.get("file:///" + os.path.abspath(html_file))
    
    time.sleep(2) 
    
    driver.set_window_size(800, 820)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'report2.html'
    png_file_path = 'report2.png'

    html_to_png(html_file_path, png_file_path)
    

secret_key=os.environ['secret_key']

slack_token = secret_key
client = WebClient(token=slack_token)

channel=['C07V24TDK7T']    

for i  in channel:
    image_path = png_file_path
    channel=i
    try:
        response = client.files_upload(
            channels=channel,
            file=image_path,
            title=f'''_Disbursment Report (SM-Wise)_
            ''',
            initial_comment=f'''            '''
        )

        if response['ok']:
            print("Image sent successfully!")
        else:
            print("Failed to send image:", response['error'])

    except SlackApiError as e:
        print(f"Error sending image: {e.response['error']}")

time.sleep(2)

cell_range4 = worksheet.range("M10:U28")
data = [[cell.value for cell in row] for row in chunked(cell_range4, 9)]
data4 = pd.DataFrame(data)
data4.columns = data4.iloc[0]
data4 = data4.drop(data4.index[0]).reset_index(drop=True)
data4=data4.replace(np.nan,'')



html_table4 = data4.to_html(escape=False, index=False)

html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{
            width: fit-content;
            font-family: 'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif;
            align-content: center;
        }}

        table, th, td {{
            border: 1px solid rgb(5, 9, 30);
            border-collapse: collapse;
            text-align: center;
            text-indent: 5px;
        }}

        td {{
            max-height: fit-content;
            max-width: fit-content;
        }}

        #firstdiv {{
            border-top: 2px solid rgb(76, 104, 65);
            border-left: 2px solid rgb(76, 104, 65);
            border-right: 2px solid rgb(76, 104, 65);
            background-color: rgb(50, 91, 168);
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            padding-bottom: 2px;
            color: white;
        }}

        h3 {{
            font-size: 35px;
            margin-top: 10px;
        }}

        h4 {{
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            text-align: left;
            padding-left: 10px;
            color: #ffffff;
            background-color: rgb(5, 9, 30);
            border: 2px solid rgb(5, 9, 30);
        }}

        th {{
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            color: #ffffff;
        }}
    </style>
</head>
<body style="display: inline-flexbox; align-content: center;">
    <div id="firstdiv" style="text-align: center; background-color: black; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: auto;">
        <h3>Login Report (SM-Wise) </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Login Report (October) :</h4>
        {html_table4}
    </div>
   
</body>
</html>
"""

html_file_path = 'report4.html'

# Save the HTML content to a file
with open(html_file_path, 'w') as file:
    file.write(html_template)

print(f"HTML file saved successfully at: {html_file_path}")



def html_to_png(html_file, output_file):
    # Configure headless Chrome
    options = Options()
    options.add_argument("--headless")  # Run in headless mode, i.e., without opening a browser window
    driver = webdriver.Chrome(options=options)
    
    driver.get("file:///" + os.path.abspath(html_file))
    
    time.sleep(2) 
    
    driver.set_window_size(800, 820)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'report4.html'
    png_file_path = 'report4.png'

    html_to_png(html_file_path, png_file_path)

secret_key=os.environ['secret_key']

slack_token = secret_key
client = WebClient(token=slack_token)

channel=['C07V24TDK7T']    


for i  in channel:
    image_path = png_file_path
    channel=i
    try:
        response = client.files_upload(
            channels=channel,
            file=image_path,
            title=f'''_Login Report (SM-Wise)_
            ''',
            initial_comment=f'''            '''
        )

        if response['ok']:
            print("Image sent successfully!")
        else:
            print("Failed to send image:", response['error'])

    except SlackApiError as e:
        print(f"Error sending image: {e.response['error']}")