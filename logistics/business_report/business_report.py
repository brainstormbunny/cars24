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
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
warnings.filterwarnings("ignore")
today = datetime.datetime.now()
today_date=today.strftime('%Y-%m-%d')
yesterday = today - datetime.timedelta(days=1)
yesterday_date=yesterday.strftime('%Y-%m-%d')
yesterday_date1=yesterday.strftime('%d-%m-%Y')

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
yesterday7=yesterday_7.strftime('%d-%m-%Y')

yesterday_7=yesterday_7.strftime('%d-%b')

gsheet_auth = 'sahil_creds.json'

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)
def chunked(iterable, size):
    return [iterable[i:i+size] for i in range(0, len(iterable), size)]


sheet_url = 'https://docs.google.com/spreadsheets/d/1k1N-MlbvjSuqueyaG1ul5mh64JfIjFHBjwWrb93_oU0/edit#gid=849813055'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("Daily_Report")
cell_range1 = worksheet.range("A3:T24")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 20)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace(np.nan,'')

def highlight_dealer_row(row, a=['Business Dependency', 'Customer Dependency', 'Docoment Issue', 'Logistics Issue', 'Tech Issue', 'Other']):
    if row['CSP Performance'] in a:
        return ['background-color: darkgrey']*len(row)
    else:
        return ['']*len(row)

# Apply the function to each row
styled_df = data1.style.apply(highlight_dealer_row, axis=1)

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
        <h3>CSP Perfomance </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>CSP Report From- {yesterday7}  To-{yesterday_date1} :</h4>
        {html_table}
    </div>
   
</body>
</html>
"""

html_file_path = 'CSP_dail_report.html'

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
    
    driver.set_window_size(1200, 835)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'CSP_dail_report.html'
    png_file_path = 'CSP_dail_report.png'

    html_to_png(html_file_path, png_file_path)

sdb = data1.loc[data1['CSP Performance'] == 'Same Day Breached', 'Overall'].iloc[0]
impacted = data1.loc[data1['CSP Performance'] == 'CSP Performance', 'Overall'].iloc[0]
impacted=float(impacted.strip('%'))
impacted = round((100 - impacted),0)
impacted = str(impacted) + '%'
bd=data1.loc[data1['CSP Performance']=='Business Dependency','Overall'].iloc[0]
bdp=data1.loc[data1['CSP Performance']=='Business Dependency','Overall%'].iloc[0]
cd=data1.loc[data1['CSP Performance']=='Customer Dependency','Overall'].iloc[0]
cdp=data1.loc[data1['CSP Performance']=='Customer Dependency','Overall%'].iloc[0]
di=data1.loc[data1['CSP Performance']=='Docoment Issue','Overall'].iloc[0]
dip=data1.loc[data1['CSP Performance']=='Docoment Issue','Overall%'].iloc[0]
li=data1.loc[data1['CSP Performance']=='Logistics Issue','Overall'].iloc[0]
lip=data1.loc[data1['CSP Performance']=='Logistics Issue','Overall%'].iloc[0]
ti=data1.loc[data1['CSP Performance']=='Tech Issue','Overall'].iloc[0]
tip=data1.loc[data1['CSP Performance']=='Tech Issue','Overall%'].iloc[0]
oi=data1.loc[data1['CSP Performance']=='Other','Overall'].iloc[0]
oip=data1.loc[data1['CSP Performance']=='Other','Overall%'].iloc[0]

secret_key=os.environ['secret_key']
slack_token = secret_key

client = WebClient(token=slack_token)

channel = 'C06HR7PBTHP'
image_path = png_file_path

try:
    response = client.files_upload(
        channels=channel,
        file=image_path,
        title=f'''_Logistics_CSP Report_
        ''',
        initial_comment=f'''*_Logistics_CSP Report_*\n
        Last 7 days RCA report:\n
        Total Breach: {sdb}\n
        Performance is Impacted :{impacted} \n
        RCA :\n
        Business Dependency :{bd} ~{bdp}\n
        Customer Dependency : {cd} ~{cdp}\n
        Docoment Issues :{di} ~{dip} \n
        Logistics Issue : {li} ~{lip} \n
        Tech Issue : {ti} ~{tip} \n
        Other : {oi} ~{oip}
        
        '''
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image:", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")
