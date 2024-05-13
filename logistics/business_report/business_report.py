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

# channel = ['C06HR7PBTHP','C06HP83G1GC']


# channel = ['C05P9MNRC3T','C06LUMTTLRL']


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
data1=data1.replace('0.0%','')
data1=data1.replace('0','')

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


# channel = ['C05P9MNRC3T','C06LUMTTLRL']
channel='C05P9MNRC3T'
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
        Other : {oi} ~{oip}\n
        '''
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image:", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")


channel='C06LUMTTLRL'
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
        Other : {oi} ~{oip}\n
        '''
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image:", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")


###########################CHD Report######################################################################


sheet_url = 'https://docs.google.com/spreadsheets/d/1du4ATpQR3unCbPTVjpuZsnyD-lvmWN5t2S_Vy3-tuYw/edit#gid=828135513'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("CHD_Report")
cell_range2 = worksheet.range("A3:V26")
data2 = [[cell.value for cell in row] for row in chunked(cell_range2, 22)]
data2 = pd.DataFrame(data2)
data2.columns = data2.iloc[0]
data2 = data2.drop(data2.index[0]).reset_index(drop=True)
data2=data2.replace(np.nan,'')
data2=data2.replace('0.0%','')
data2=data2.replace('0','')



def highlight_dealer_row(row, a=['Inspection Issue', 'Yard Issue', 'Logistics Issue', 'Dealer Dependency', 'Sales Issue']):
    if row['CHD Performance RCA'] in a:
        return ['background-color: darkgrey']*len(row)
    else:
        return ['']*len(row)

# Apply the function to each row
styled_df1 = data2.style.apply(highlight_dealer_row, axis=1)

html_table1 = styled_df1.to_html(escape=False, index=False)

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
        <h3>CHD Performance RCA </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>CHD Performance Report From- {yesterday7}  To-{yesterday_date1} :</h4>
        {html_table1}
    </div>
   
</body>
</html>
"""

html_file_path1 = 'Chd_dail_report.html'

# Save the HTML content to a file
with open(html_file_path1, 'w') as file:
    file.write(html_template)

print(f"HTML file saved successfully at: {html_file_path}")



def html_to_png(html_file, output_file):
    # Configure headless Chrome
    options = Options()
    options.add_argument("--headless")  # Run in headless mode, i.e., without opening a browser window
    driver = webdriver.Chrome(options=options)
    
    driver.get("file:///" + os.path.abspath(html_file))
    
    time.sleep(2) 
    
    driver.set_window_size(1250, 1198)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path1 = 'Chd_dail_report.html'
    png_file_path1 = 'Chd_dail_report.png'

    html_to_png(html_file_path1, png_file_path1)

    

data2['CHD Performance RCA'] = data2['CHD Performance RCA'].str.strip()

sdb1 = data2.loc[data2['CHD Performance RCA'] == '24 Hours TAT Breach', 'Overall'].iloc[0]

impacted1 = data2.loc[data2['CHD Performance RCA'] == 'CHD Performance', 'Overall'].iloc[0]
impacted1=float(impacted1.strip('%'))
impacted1 = round((100 - impacted1),0)
impacted1 = str(impacted1) + '%'

ii=data2.loc[data2['CHD Performance RCA']=='Inspection Issue','Overall'].iloc[0]
iip=data2.loc[data2['CHD Performance RCA']=='Inspection Issue','Overall%'].iloc[0]

yi=data2.loc[data2['CHD Performance RCA']=='Yard Issue','Overall'].iloc[0]
yip=data2.loc[data2['CHD Performance RCA']=='Yard Issue','Overall%'].iloc[0]

li1=data2.loc[data2['CHD Performance RCA']=='Logistics Issue','Overall'].iloc[0]
lip1=data2.loc[data2['CHD Performance RCA']=='Logistics Issue','Overall%'].iloc[0]

dd=data2.loc[data2['CHD Performance RCA']=='Dealer Dependency','Overall'].iloc[0]
ddp=data2.loc[data2['CHD Performance RCA']=='Dealer Dependency','Overall%'].iloc[0]

si=data2.loc[data2['CHD Performance RCA']=='Sales Issue','Overall'].iloc[0]
sip=data2.loc[data2['CHD Performance RCA']=='Sales Issue','Overall%'].iloc[0]

client = WebClient(token=slack_token)



image_path = png_file_path1
channel='C05P9MNRC3T'
try:
    response = client.files_upload(
        channels=channel,
        file=image_path,
        title=f'''_Logistics_CHD Report_
        ''',
        initial_comment=f'''*_Logistics_CHD Report_*\n
        Last 7 days RCA report:\n
        Total Breach: {sdb1}\n
        Performance is Impacted :{impacted1} \n
        RCA :\n
        Inspection Issue :{ii} ~{iip}\n
        Yard Issue  : {yi} ~{yip}\n
        Logistics Issue :{li1} ~{lip1} \n
        Dealer Dependency  : {dd} ~{ddp} \n
        Sale Issue : {si} ~{sip} \n
        '''
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image:", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")


image_path = png_file_path1
channel='C06LUMTTLRL'
try:
    response = client.files_upload(
        channels=channel,
        file=image_path,
        title=f'''_Logistics_CHD Report_
        ''',
        initial_comment=f'''*_Logistics_CHD Report_*\n
        Last 7 days RCA report:\n
        Total Breach: {sdb1}\n
        Performance is Impacted :{impacted1} \n
        RCA :\n
        Inspection Issue :{ii} ~{iip}\n
        Yard Issue  : {yi} ~{yip}\n
        Logistics Issue :{li1} ~{lip1} \n
        Dealer Dependency  : {dd} ~{ddp} \n
        Sale Issue : {si} ~{sip} \n
        '''
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image:", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")


#########################################eVTF###############################################################
sheet_url = 'https://docs.google.com/spreadsheets/d/1du4ATpQR3unCbPTVjpuZsnyD-lvmWN5t2S_Vy3-tuYw/edit#gid=828135513'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("eVTF Report")
cell_range3 = worksheet.range("A3:V21")
data3 = [[cell.value for cell in row] for row in chunked(cell_range3, 22)]
data3 = pd.DataFrame(data3)
data3.columns = data3.iloc[0]
data3 = data3.drop(data3.index[0]).reset_index(drop=True)
data3=data3.replace(np.nan,'')

data3=data3.replace('0.0%','')
data3=data3.replace('0','')



def highlight_dealer_row(row, a=['Dealer Dependency', 'Tech Issue', 'Logistics Issue', 'Other']):
    if row['eVTF Performance RCA'] in a:
        return ['background-color: darkgrey']*len(row)
    else:
        return ['']*len(row)

# Apply the function to each row
styled_df2 = data3.style.apply(highlight_dealer_row, axis=1)

html_table2 = styled_df2.to_html(escape=False, index=False)

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
        <h3>eVTF Performance RCA </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>eVTF Performance Report From- {yesterday7}  To-{yesterday_date1} :</h4>
        {html_table2}
    </div>
   
</body>
</html>
"""

html_file_path2 = 'eVTF_dail_report.html'

# Save the HTML content to a file
with open(html_file_path2, 'w') as file:
    file.write(html_template)

print(f"HTML file saved successfully at: {html_file_path2}")

    
def html_to_png(html_file, output_file):
    # Configure headless Chrome
    options = Options()
    options.add_argument("--headless")  # Run in headless mode, i.e., without opening a browser window
    driver = webdriver.Chrome(options=options)
    
    driver.get("file:///" + os.path.abspath(html_file))
    
    time.sleep(2) 
    
    driver.set_window_size(1270, 800)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path2 = 'eVTF_dail_report.html'
    png_file_path2 = 'eVTF_dail_report.png'

    html_to_png(html_file_path2, png_file_path2)


data3['eVTF Performance RCA'] = data3['eVTF Performance RCA'].str.strip()

sdb2 = data3.loc[data3['eVTF Performance RCA'] == 'eVTF Unverified', 'Overall'].iloc[0]

impacted2 = data3.loc[data3['eVTF Performance RCA'] == 'eVTF Performance', 'Overall'].iloc[0]
impacted2=float(impacted2.strip('%'))
impacted2 = round((100 - impacted2),0)
impacted2 = str(impacted2) + '%'


dd1=data3.loc[data3['eVTF Performance RCA']=='Dealer Dependency','Overall'].iloc[0]
# ddp1=data3.loc[data['eVTF Performance RCA']=='Dealer Dependency','Overall%'].iloc[0]
ddp1 = data3.loc[data3['eVTF Performance RCA'] == 'Dealer Dependency', 'Overall%'].iloc[0]

tii=data3.loc[data3['eVTF Performance RCA']=='Tech Issue','Overall'].iloc[0]
ti2p=data3.loc[data3['eVTF Performance RCA']=='Tech Issue','Overall%'].iloc[0]
li2=data3.loc[data3['eVTF Performance RCA']=='Logistics Issue','Overall'].iloc[0]
lip2=data3.loc[data3['eVTF Performance RCA']=='Logistics Issue','Overall%'].iloc[0]
li1=data3.loc[data3['eVTF Performance RCA']=='Logistics Issue','Overall'].iloc[0]
lip1=data3.loc[data3['eVTF Performance RCA']=='Logistics Issue','Overall%'].iloc[0]
o1=data3.loc[data3['eVTF Performance RCA']=='Other','Overall'].iloc[0]
op1=data3.loc[data3['eVTF Performance RCA']=='Other','Overall%'].iloc[0]


client = WebClient(token=slack_token)


channel='C05P9MNRC3T'
image_path = png_file_path2

try:
    response = client.files_upload(
        channels=channel,
        file=image_path,
        title=f'''_Logistics_eVTF_Report_
        ''',
        initial_comment=f'''*_Logistics_eVTF_Report_*\n
        Last 7 days RCA report:\n
        Unverified cases : {sdb2}\n
        Performance is Impacted :{impacted2} \n
        RCA :\n
        Dealer Dependency :{dd1} ~{ddp1}\n
        Tech Issue  : {tii} ~{ti2p}\n
        Logistics Issue :{li2} ~{lip2} \n
        Other  : {o1} ~{op1} \n
        '''
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image:", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")

channel='C06LUMTTLRL'
image_path = png_file_path2

try:
    response = client.files_upload(
        channels=channel,
        file=image_path,
        title=f'''_Logistics_eVTF_Report_
        ''',
        initial_comment=f'''*_Logistics_eVTF_Report_*\n
        Last 7 days RCA report:\n
        Unverified cases : {sdb2}\n
        Performance is Impacted :{impacted2} \n
        RCA :\n
        Dealer Dependency :{dd1} ~{ddp1}\n
        Tech Issue  : {tii} ~{ti2p}\n
        Logistics Issue :{li2} ~{lip2} \n
        Other  : {o1} ~{op1} \n
        '''
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image:", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")



###################Driver QC##################################

sheet_url = 'https://docs.google.com/spreadsheets/d/1xuboT__o4o7sIA53-MrVUbXERMFRv2M2AY1q9PYS9YM/edit#gid=120478345'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("Report1")
cell_range4 = worksheet.range("B4:V29")
data4 = [[cell.value for cell in row] for row in chunked(cell_range4, 21)]
data4 = pd.DataFrame(data4)
data4.columns = data4.iloc[0]
data4 = data4.drop(data4.index[0]).reset_index(drop=True)
data4=data4.replace(np.nan,'')

data4=data4.replace('0.0%','')



def highlight_dealer_row(row, a=['Link Issue', 'Towing Issue', 'CJ Dependency', 'Logistics Issue','Business Approval','Other Issue']):
    if row['Driver QC'] in a:
        return ['background-color: darkgrey']*len(row)
    else:
        return ['']*len(row)

# Apply the function to each row
styled_df4 = data4.style.apply(highlight_dealer_row, axis=1)
html_table4 = styled_df4.to_html(escape=False, index=False)

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
        <h3>Driver QC RCA </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Driver QC Report From- {yesterday7}  To-{yesterday_date1} :</h4>
        {html_table4}
    </div>
   
</body>
</html>
"""

html_file_path4 = 'Driver_QC.html'

# Save the HTML content to a file
with open(html_file_path4, 'w') as file:
    file.write(html_template)

print(f"HTML file saved successfully at: {html_file_path4}")

    
def html_to_png(html_file, output_file):
    # Configure headless Chrome
    options = Options()
    options.add_argument("--headless")  # Run in headless mode, i.e., without opening a browser window
    driver = webdriver.Chrome(options=options)
    
    driver.get("file:///" + os.path.abspath(html_file))
    
    time.sleep(2) 
    
    driver.set_window_size(1270, 900)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path4 = 'Driver_QC.html'
    png_file_path4 = 'Driver_QC.png'

    html_to_png(html_file_path4, png_file_path4)


data4['Driver QC'] = data4['Driver QC'].str.strip()

sdb4 = data4.loc[data4['Driver QC'] == 'Pending RCA', 'Overall'].iloc[0]

impacted4 = data4.loc[data4['Driver QC'] == 'Driver QC Adherence', 'Overall'].iloc[0]
impacted4=float(impacted4.strip('%'))
impacted4 = round((100 - impacted4),0)
impacted4 = str(impacted4) + '%'

lii4=data4.loc[data4['Driver QC']=='Link Issue','Overall'].iloc[0]
lii4p=data4.loc[data4['Driver QC']=='Link Issue','Overall%'].iloc[0]
prca4=data4.loc[data4['Driver QC'] == 'Pending RCA', 'Overall'].iloc[0]

ti4=data4.loc[data4['Driver QC']=='Towing Issue','Overall'].iloc[0]
ti4p=data4.loc[data4['Driver QC']=='Towing Issue','Overall%'].iloc[0]

cd4=data4.loc[data4['Driver QC']=='CJ Dependency','Overall'].iloc[0]
cd4p=data4.loc[data4['Driver QC']=='CJ Dependency','Overall%'].iloc[0]

li4=data4.loc[data4['Driver QC']=='Logistics Issue','Overall'].iloc[0]
li4p=data4.loc[data4['Driver QC']=='Logistics Issue','Overall%'].iloc[0]

ba4=data4.loc[data4['Driver QC']=='Business Approval','Overall'].iloc[0]
ba4p=data4.loc[data4['Driver QC']=='Business Approval','Overall%'].iloc[0]

oi4=data4.loc[data4['Driver QC']=='Other Issue','Overall'].iloc[0]
oi4p=data4.loc[data4['Driver QC']=='Other Issue','Overall%'].iloc[0]

client = WebClient(token=slack_token)

channel = 'C06LUMTTLRL'
image_path = png_file_path4

try:
    response = client.files_upload(
        channels=channel,
        file=image_path,
        title=f'''_Logistics_Driver_QC
        ''',
        initial_comment=f'''*_Logistics_Driver_QC*\n
        Last 7 days RCA report:\n
        Pending RCA cases : {prca4}\n

        Performance is Impacted :{impacted4} \n
        RCA :\n
        Link Issue :{lii4} ~{lii4p}\n
        Towing Issue  : {ti4} ~{ti4p}\n
        Logistics Issue :{li4} ~{li4p} \n
        Link Issue :{lii4} ~{lii4p}\n
        Business Approval :{ba4} ~{ba4p}\n
        Other Issue :{oi4} ~{oi4p}\n
        '''
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image:", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")


channel = 'C06LUMTTLRL'
image_path = png_file_path4

try:
    response = client.files_upload(
        channels=channel,
        file=image_path,
        title=f'''_Logistics_Driver_QC
        ''',
        initial_comment=f'''*_Logistics_Driver_QC*\n
        Last 7 days RCA report:\n
        Pending RCA cases : {prca4}\n

        Performance is Impacted :{impacted4} \n
        RCA :\n
        Link Issue :{lii4} ~{lii4p}\n
        Towing Issue  : {ti4} ~{ti4p}\n
        Logistics Issue :{li4} ~{li4p} \n
        Link Issue :{lii4} ~{lii4p}\n
        Business Approval :{ba4} ~{ba4p}\n
        Other Issue :{oi4} ~{oi4p}\n
        '''
    )

    if response['ok']:
        print("Image sent successfully!")
    else:
        print("Failed to send image:", response['error'])

except SlackApiError as e:
    print(f"Error sending image: {e.response['error']}")

