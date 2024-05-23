import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
import numpy as np
import gspread_dataframe as gd
import os
import time
import datetime
import warnings
import sys
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
warnings.filterwarnings("ignore")
today = datetime.datetime.now()
today_date=today.strftime('%Y-%m-%d')
yesterday = today - datetime.timedelta(days=1)
yesterday_date=yesterday.strftime('%Y-%m-%d')
yesterday_date1=yesterday.strftime('%d-%B-%Y')
import datetime
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
yesterday7=yesterday_7.strftime('%d-%B-%Y')
secret_key=os.environ['secret_key']

yesterday_7=yesterday_7.strftime('%d-%b')

# sys.exit()

gsheet_auth = 'sahil_creds.json'



scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)
def chunked(iterable, size):
    return [iterable[i:i+size] for i in range(0, len(iterable), size)]


sheet_url = 'https://docs.google.com/spreadsheets/d/1fo8q9ivgLykEcyeSbZwVfBznjq39yc3qkoDjEhua4mI/edit#gid=1940626723'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("Report")
cell_range1 = worksheet.range("A1:E24")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 5)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace(np.nan,'')

data1.columns=[col.strip() for col in data1.columns]

data1['Achieved'] = pd.to_numeric(data1['Achieved'].str.rstrip('%'), errors='coerce')
data1['Achieved'] = data1['Achieved'].replace(np.nan, 0)
def highlight_score(val):
    color = None
    if val >= 1 and val <= 40:
        color = 'rgba(255, 0, 0, 0.5)'  # Red with 50% transparency
    elif val >= 41 and val <= 60:
        color = 'rgba(255, 165, 0, 0.5)'  # Yellow with 50% transparency
    elif val > 60:
        color = 'rgba(0, 255, 0, 0.5)'  # Green with 50% transparency
    elif val == 0:
        color = 'rgba(255, 255, 255, 1)'  # White with 100% transparency    
    return f'background-color: {color}' if color else ''


styled_df = data1.style.applymap(highlight_score, subset=['Achieved'])
styled_df = styled_df.format({'Achieved': '{:.2f}%'})  
html_table = styled_df.to_html(escape=False, index=False)

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
        <h3>Customer Review Report </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4> Customer Review:</h4>
        {html_table}
    </div>
   
</body>
</html>
"""

html_file_path = 'cx_review.html'
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
    
    driver.set_window_size(560, 770)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'cx_review.html'
    png_file_path = 'cx_review.png'

    html_to_png(html_file_path, png_file_path)



secret_key=os.environ['secret_key']
slack_token = secret_key


client = WebClient(token=slack_token)


channel=['C06LUMTTLRL']
for i  in channel:
    image_path = png_file_path
    channel=i
    try:
        response = client.files_upload(
            channels=channel,
            file=image_path,
            title=f'''Customer Review Report
            ''',
            initial_comment=f'''            '''
        )

        if response['ok']:
            print("Image sent successfully!")
        else:
            print("Failed to send image:", response['error'])

    except SlackApiError as e:
        print(f"Error sending image: {e.response['error']}")

