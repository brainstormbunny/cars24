import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
import numpy as np
import gspread_dataframe as gd
import os
import time
import datetime
import datetime
import warnings
import sys
import imgkit
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
warnings.filterwarnings("ignore")
today = datetime.datetime.now()
today_date=today.strftime('%Y-%m-%d')
yesterday = today - datetime.timedelta(days=3)
yesterday_date=yesterday.strftime('%Y-%m-%d')
yesterday_date1=yesterday.strftime('%d-%B-%Y')
today = datetime.datetime.now()
today_date=today.strftime('%Y-%m-%d')
yesterday = today - datetime.timedelta(days=1)
yesterday_date=yesterday.strftime('%d-%b')
yesterday_2 = today - datetime.timedelta(days=2)
yesterday_2=yesterday_2.strftime('%d-%b')
yesterday_3 = today - datetime.timedelta(days=3)
yesterday_3=yesterday_3.strftime('%d-%b')
yesterday_4 = today - datetime.timedelta(days=4)
yesterday_4=yesterday_4.strftime('%d-%b')
yesterday_5 = today - datetime.timedelta(days=5)
yesterday_5=yesterday_5.strftime('%d-%b')
yesterday_6 = today - datetime.timedelta(days=6)
yesterday_6=yesterday_6.strftime('%d-%b')
yesterday_7 = today - datetime.timedelta(days=7)
yesterday_7=yesterday_7.strftime('%d-%b')
gsheet_auth = 'sahil_creds.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)


ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1wZGLobMn-uVuz2BUIwPKH-Zm-xQlHVpnsY0HaqEoY80/edit#gid=0').worksheet('Raw_Data')
# gd.set_with_dataframe(ws,df2,resize=True,row=1,col=1)  #write
df=pd.DataFrame(ws.get_all_records())  
print(df)

report=df[['REQUESTED_PICKUP_TIME','APPOINTMENTID','REGISTRATION_NUMBER','MAKE','MODEL','FROM_CITY','FROM_LOCATION','TO_CITY']]
kochi=report[report['TO_CITY']=='Kochi']
print(kochi)


html_table = kochi.to_html(index=False)


# HTML template
html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            width: fit-content;
            font-family: 'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif;
            align-content: center;
        }

        table, th, td {
            border: 1px solid rgb(5, 9, 30);
            border-collapse: collapse;
            text-align: center;
            text-indent: 5px;
        }

        td {
            max-height: fit-content;
            max-width: fit-content;
        }

        #firstdiv {
            border-top: 2px solid rgb(76, 104, 65);
            border-left: 2px solid rgb(76, 104, 65);
            border-right: 2px solid rgb(76, 104, 65);
            background-color: rgb(50, 91, 168);
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            padding-bottom: 2px;
            color: white;
        }

        h3 {
            font-size: 30px;
            margin-top: 10px;
        }

        h4 {
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            text-align: left;
            padding-left: 10px;
            color: #ffffff;
            background-color: rgb(5, 9, 30);
            border: 2px solid rgb(5, 9, 30);
        }

        th {
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            color: #ffffff;
        }
    </style>
</head>
<body style="display: inline-flexbox; align-content: center;">
    <div id="firstdiv" style="text-align: center; background-color: black; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: auto;">
        <h3>PTS Pendency Report </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>PTS Pendency Report-Kochi :</h4>
        {table_content}
    </div>
   
</body>
</html>
"""

# Replace {table_content} with the HTML table
html_text = html_template.replace('{table_content}',html_table  )

html_file_path = 'test_report.html'

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
    
    driver.set_window_size(1280, 1000)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'test_report.html'
    png_file_path = 'test_report.png'

    html_to_png(html_file_path, png_file_path)

secret_key=os.environ['secret_key']

slack_token = secret_key

client = WebClient(token=slack_token)

channel = 'C01KBQLG0J2' 
image_path=png_file_path
try:
    response = client.files_upload(
        channels=channel,
        file=image_path,
        title='Image title',
        initial_comment='Daily Report'
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image: ", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")

gj = report[report['TO_CITY'].isin(['Hyderabad', 'Rajkot', 'Ahmedabad', 'Surat', 'Vadodara'])]
print(gj)


html_table1 = gj.to_html(index=False)

# HTML template
html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            width: fit-content;
            font-family: 'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif;
            align-content: center;
        }

        table, th, td {
            border: 1px solid rgb(5, 9, 30);
            border-collapse: collapse;
            text-align: center;
            text-indent: 5px;
        }

        td {
            max-height: fit-content;
            max-width: fit-content;
        }

        #firstdiv {
            border-top: 2px solid rgb(76, 104, 65);
            border-left: 2px solid rgb(76, 104, 65);
            border-right: 2px solid rgb(76, 104, 65);
            background-color: rgb(50, 91, 168);
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            padding-bottom: 2px;
            color: white;
        }

        h3 {
            font-size: 30px;
            margin-top: 10px;
        }

        h4 {
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            text-align: left;
            padding-left: 10px;
            color: #ffffff;
            background-color: rgb(5, 9, 30);
            border: 2px solid rgb(5, 9, 30);
        }

        th {
            background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
            color: #ffffff;
        }
    </style>
</head>
<body style="display: inline-flexbox; align-content: center;">
    <div id="firstdiv" style="text-align: center; background-color: black; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: auto;">
        <h3>PTS Pendency Report </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>PTS Pendency Report-(Hyderabad,Rajkot,Ahmedabad,Vadodara,Surat) :</h4>
        {table_content}
    </div>
   
</body>
</html>
"""

# Replace {table_content} with the HTML table
html_text1 = html_template.replace('{table_content}',html_table1  )



html_file_path = 'test_report.html'

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
    
    driver.set_window_size(1280, 1000)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'test_report.html'
    png_file_path = 'test_report.png'

    html_to_png(html_file_path, png_file_path)

secret_key=os.environ['secret_key']

slack_token = secret_key


client = WebClient(token=slack_token)

channel = 'C06JQ4FMYA3' 
image_path=png_file_path
try:
    response = client.files_upload(
        channels=channel,
        file=image_path,
        title='Image title',
        initial_comment='Daily Report'
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image: ", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")

