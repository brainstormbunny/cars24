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

def chunked(iterable, size):
    return [iterable[i:i+size] for i in range(0, len(iterable), size)]
gsheet_auth='sahil_creds.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)

secret_key=os.environ['secret_key']
slack_token = secret_key
client = WebClient(token=slack_token)

sheet_url11 = 'https://docs.google.com/spreadsheets/d/144xWGvX7ipabfIkQUvIdzZY_wbwDLwzfjVLyoUmLvDA/edit?gid=1960642873#gid=1960642873'
sheet = gc.open_by_url(sheet_url11)
worksheet = sheet.worksheet("Form Responses 4")
cell_range11 = worksheet.range("A1:AO")
data = [[cell.value for cell in row] for row in chunked(cell_range11, 41)]
data11 = pd.DataFrame(data)
data11.columns = data11.iloc[0]
data11 = data11.drop(data11.index[0]).reset_index(drop=True)
data11=data11.replace(np.nan,'')
data11['Loan ID'] = data11['Loan ID'].replace('', np.nan)
data11 = data11.dropna(subset=['Loan ID'])
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1lyQNBy4VeBix3AKGQss_U0RWyF2zYe56k7pqSfHeG3U/edit?gid=1960642873#gid=1960642873').worksheet('Form Responses 4')
ws.batch_clear(['A1:AO'])
gd.set_with_dataframe(ws,data11,resize=True,row=1,col=1)  
df1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1tZL5V-w8gu6SMRCc6Hzm0IkkOfVXjjZ55ltaMdngOtg/edit?gid=328438549#gid=328438549').worksheet('Processed_Data')
df1 = pd.DataFrame(df1.get_all_records())
df1=df1[['LEAD_ID','LOAN_APPLICATION_ID','CONTACT_NUMBER','LATEST_LEAD_CREATION_TIMESTAMP','LOGIN_DATE','FINAL_TENURE','FINAL_ROI','FINAL_DISBURSABLE_LOAN_AMOUNT','FINAL_TOTAL_LOAN_AMOUNT','CREDIT_APPROVAL_FLAG','CHANNEL_DS','DC_FTR','TOTAL_LTV','CREDIT_APPROVED_TIMESTAMP','TNC_ACCEPTED_TIMESTAMP','LEAD_MODIFIED_BY','DEALER_CITY','DEALER_CODE2','CASE_STATUS','DISBURSED_TIME','HPA_STATUS','LAST_RISK_BUCKET','Final Dealer code','DEALER_CITY','Final FOS','Credit LTV','DS_ROI','DS_CHANNEL']]
df1 = df1[~df1['LEAD_ID'].isna()]
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1lyQNBy4VeBix3AKGQss_U0RWyF2zYe56k7pqSfHeG3U/edit?gid=511880551#gid=511880551').worksheet('Simpler_data_raw')
ws.batch_clear(['A1:AB'])
gd.set_with_dataframe(ws,df1,resize=False,row=1,col=1)  


sheet_url = 'https://docs.google.com/spreadsheets/d/1lyQNBy4VeBix3AKGQss_U0RWyF2zYe56k7pqSfHeG3U/edit?gid=511880551#gid=511880551'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("All_reports")
cell_range1 = worksheet.range("E3:W23")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 19)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace(np.nan,'')


html_table1 = data1.to_html(escape=False, index=False)

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
        <h3>Overall Channel Wise Report</h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Overall Channel Wise Report:</h4>
        {html_table1}
    </div>


</body>
</html>
"""

html_file_path = 'overall_report.html'

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
    
    driver.set_window_size(1450, 1000)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'overall_report.html'
    png_file_path = 'overall_report.png'
    html_to_png(html_file_path, png_file_path)

channel=['C06HR7PBTHP']
for i  in channel:
    image_path = png_file_path
    channel=i
    try:
        response = client.files_upload(
            channels=channel,
            file=image_path,
            title=f'''Report_
            ''',
            initial_comment=f'''            '''
        )

        if response['ok']:
            print("Image sent successfully!")
        else:
            print("Failed to send image:", response['error'])

    except SlackApiError as e:
        print(f"Error sending image: {e.response['error']}")


sheet_url = 'https://docs.google.com/spreadsheets/d/1lyQNBy4VeBix3AKGQss_U0RWyF2zYe56k7pqSfHeG3U/edit?gid=511880551#gid=511880551'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("All_reports")
cell_range1 = worksheet.range("E26:W44")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 19)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace(np.nan,'')


html_table1 = data1.to_html(escape=False, index=False)

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
        <h3>Dealer Channel Wise Report</h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Dealer Channel Wise Report:</h4>
        {html_table1}
    </div>


</body>
</html>
"""

html_file_path = 'Dealer.html'

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
    
    driver.set_window_size(1450, 1000)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'Dealer.html'
    png_file_path = 'Dealer.png'
    html_to_png(html_file_path, png_file_path)


channel=['C06HR7PBTHP']
for i  in channel:
    image_path = png_file_path
    channel=i
    try:
        response = client.files_upload(
            channels=channel,
            file=image_path,
            title=f'''Report_
            ''',
            initial_comment=f'''            '''
        )

        if response['ok']:
            print("Image sent successfully!")
        else:
            print("Failed to send image:", response['error'])

    except SlackApiError as e:
        print(f"Error sending image: {e.response['error']}")


sheet_url = 'https://docs.google.com/spreadsheets/d/1lyQNBy4VeBix3AKGQss_U0RWyF2zYe56k7pqSfHeG3U/edit?gid=511880551#gid=511880551'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("All_reports")
cell_range1 = worksheet.range("E49:W67")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 19)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace(np.nan,'')


html_table1 = data1.to_html(escape=False, index=False)

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
        <h3>DSA Channel Wise Report</h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>DSA Channel Wise Report:</h4>
        {html_table1}
    </div>


</body>
</html>
"""

html_file_path = 'DSA.html'

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
    
    driver.set_window_size(1450, 1000)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'DSA.html'
    png_file_path = 'DSA.png'
    html_to_png(html_file_path, png_file_path)

channel=['C06HR7PBTHP']
for i  in channel:
    image_path = png_file_path
    channel=i
    try:
        response = client.files_upload(
            channels=channel,
            file=image_path,
            title=f'''Report_
            ''',
            initial_comment=f'''            '''
        )

        if response['ok']:
            print("Image sent successfully!")
        else:
            print("Failed to send image:", response['error'])

    except SlackApiError as e:
        print(f"Error sending image: {e.response['error']}")


sheet_url = 'https://docs.google.com/spreadsheets/d/1lyQNBy4VeBix3AKGQss_U0RWyF2zYe56k7pqSfHeG3U/edit?gid=511880551#gid=511880551'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("All_reports")
cell_range1 = worksheet.range("E70:W78")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 19)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace(np.nan,'')


html_table1 = data1.to_html(escape=False, index=False)

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
        <h3>C2B Channel Wise Report</h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>C2B Channel Wise Report:</h4>
        {html_table1}
    </div>


</body>
</html>
"""

html_file_path = 'C2B.html'

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
    
    driver.set_window_size(1450, 800)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'C2B.html'
    png_file_path = 'C2B.png'
    html_to_png(html_file_path, png_file_path)


channel=['C06HR7PBTHP']
for i  in channel:
    image_path = png_file_path
    channel=i
    try:
        response = client.files_upload(
            channels=channel,
            file=image_path,
            title=f'''Report_
            ''',
            initial_comment=f'''            '''
        )

        if response['ok']:
            print("Image sent successfully!")
        else:
            print("Failed to send image:", response['error'])

    except SlackApiError as e:
        print(f"Error sending image: {e.response['error']}")
