import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
import numpy as np
from gmail_functions import create_message_without_attachment
from slack_sdk import WebClient
import gspread_dataframe as gd
import os
import time
import datetime
import imgkit
from slack_sdk.errors import SlackApiError
import warnings
import sys
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
gsheet_auth='sahil_creds.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)


ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/17ZEHIYeo9lKWvRP2-vKQ6UdeoY9Nnxwz8K9UhcPLNqI/edit#gid=0').worksheet('Master_data')
# gd.set_with_dataframe(ws,df,resize=False,row=1,col=1)  #write
df=pd.DataFrame(ws.get_all_records())  

df1=df[['LEAD_ID','REGISTRATION_NUMBER','MAKE','MODEL','PARKING_CITY','LATEST_PARKING_YARD','SALE_CONFIRMED_DATE','INTRANSIT_DATE','PICKUP_REGION_NAME','TRANSPORTATION_CHARGE','STOCKIN2_DATE','GFD_STATUS','CURRENT_CAR_STATUS','LANE_CONCAT','C24_QUOTE','REGISTRATION_YEAR','AGING_YEAR_FROM_REGISTRATION_YEAR','FLAG']]


df1['Mapping_Date']=today_date
df1['Transporter_Name']=''


ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1wZGLobMn-uVuz2BUIwPKH-Zm-xQlHVpnsY0HaqEoY80/edit#gid=0').worksheet('Raw_Data')
# gd.set_with_dataframe(ws,df,resize=False,row=1,col=1)  #write
gs1=pd.DataFrame(ws.get_all_records()) 

gs=gs1[['APPOINTMENTID','REQUESTED_PICKUP_TIME']]
gs = gs.rename(columns={'APPOINTMENTID': 'LEAD_ID'})
gs['C2B/GS'] = 'GS'

gs['LEAD_ID']=gs['LEAD_ID'].astype(str)
df1['LEAD_ID']=df1['LEAD_ID'].astype(str)


data = df1.merge(gs, on='LEAD_ID', how='left')
data['C2B/GS'] = data['C2B/GS'].replace(np.nan,'C2B')
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/17ZEHIYeo9lKWvRP2-vKQ6UdeoY9Nnxwz8K9UhcPLNqI/edit#gid=0').worksheet('Master_data1')
# gd.set_with_dataframe(ws,data,resize=False,row=1,col=1)  #write
md=pd.DataFrame(ws.get_all_records())  
md['Mapping_Date']=''

md_dd=md[['LEAD_ID','REGISTRATION_NUMBER','SALE_CONFIRMED_DATE','Mapping_Date']]

md=md[['REGISTRATION_NUMBER','SALE_CONFIRMED_DATE']]
md = md.drop_duplicates()
md['check']='a'

data1=data.merge(md,on=['REGISTRATION_NUMBER','SALE_CONFIRMED_DATE'],how='left')
data1['check']=data1['check'].replace(np.nan,'b')
data1_filtered = data1.loc[data1['check'] == 'b']

data1_filtered=data1_filtered[['LEAD_ID','REGISTRATION_NUMBER','MAKE','MODEL','PARKING_CITY','LATEST_PARKING_YARD','SALE_CONFIRMED_DATE','PICKUP_REGION_NAME','GFD_STATUS','CURRENT_CAR_STATUS','LANE_CONCAT','C24_QUOTE','REGISTRATION_YEAR','AGING_YEAR_FROM_REGISTRATION_YEAR','FLAG','C2B/GS']]

gs1['LANE_CONCAT'] = gs1['FROM_CITY'] + '-' + gs1['TO_CITY']
gs1 = gs1.rename(columns={'FROM_CITY':'PARKING_CITY', 'TO_CITY': 'PICKUP_REGION_NAME','FROM_LOCATION':'LATEST_PARKING_YARD'})
gs1['GFD_STATUS']=''
gs1['CURRENT_CAR_STATUS']=''
gs1['AGING_YEAR_FROM_REGISTRATION_YEAR']=''
gs1['REGISTRATION_YEAR']=''
gs1['FLAG']=''
gs1=gs1.merge(md,on='REGISTRATION_NUMBER',how='left')

gs1=gs1.replace(np.nan,'b')
gs1=gs1.loc[gs1['check']=='b']
gs1=gs1.rename(columns={'APPOINTMENTID':'LEAD_ID'})
gs1['SALE_CONFIRMED_DATE']=''
gs1['C2B/GS']='GS'

gs1=gs1[['LEAD_ID','REGISTRATION_NUMBER','MAKE','MODEL','PARKING_CITY','LATEST_PARKING_YARD','SALE_CONFIRMED_DATE','PICKUP_REGION_NAME','GFD_STATUS','CURRENT_CAR_STATUS','LANE_CONCAT','C24_QUOTE','REGISTRATION_YEAR','AGING_YEAR_FROM_REGISTRATION_YEAR','FLAG','C2B/GS']]
gs1['PICKUP_REGION_NAME']=gs1['PICKUP_REGION_NAME'].replace('Bangalore','Bengaluru MRL')
gs1['PICKUP_REGION_NAME']=gs1['PICKUP_REGION_NAME'].replace('Indore','Indore MRL')
gs1['PICKUP_REGION_NAME']=gs1['PICKUP_REGION_NAME'].replace('Nashik','Nashik MRL')
gs1['PICKUP_REGION_NAME']=gs1['PICKUP_REGION_NAME'].replace('Agra','Agra MRL')


data_final = pd.concat([data1_filtered, gs1], ignore_index=True)
data_final = data_final[~data_final['PICKUP_REGION_NAME'].isin(['Kochi', 'Hyderabad', 'Ahmedabad', 'Rajkot','Surat','Vadodara','UP-ALL-HI-GROUPS'])]
data_final1 = data_final[~data_final['REGISTRATION_NUMBER'].isin(['MH43AB6920'])]
selected_rows = data_final1[(data_final1['PARKING_CITY'] == 'Ludhiana') & (data_final1['PICKUP_REGION_NAME'] == 'Chandigarh') | (data_final1['PARKING_CITY'] == 'Kanpur') & (data_final1['PICKUP_REGION_NAME'] == 'Lucknow')]
selected_rows=selected_rows[['LEAD_ID','REGISTRATION_NUMBER','MAKE','MODEL','PARKING_CITY','LATEST_PARKING_YARD','PICKUP_REGION_NAME','LANE_CONCAT']]
data_final1 = data_final1.drop(data_final[(data_final['PARKING_CITY'] == 'Ludhiana') & (data_final['PICKUP_REGION_NAME'] == 'Chandigarh')].index)
data_final1 = data_final1.drop(data_final[(data_final['PARKING_CITY'] == 'Muzaffarpur') & (data_final['PICKUP_REGION_NAME'] == 'Patna')].index)
data_final1 = data_final1.drop(data_final[(data_final['PARKING_CITY'] == 'Vellore') & (data_final['PICKUP_REGION_NAME'] == 'Chennai')].index)
data_final1 = data_final1.drop(data_final[(data_final['PARKING_CITY'] == 'Kanpur') & (data_final['PICKUP_REGION_NAME'] == 'Lucknow')].index)
data_final1 = data_final1.drop(data_final[(data_final['PARKING_CITY'] == 'ANKLESHWAR') & (data_final['PICKUP_REGION_NAME'] == 'BHARUCH')].index)
data_final1 = data_final1.drop(data_final[(data_final['PARKING_CITY'] == 'JUNAGADH') & (data_final['PICKUP_REGION_NAME'] == 'JUNAGADH CITY')].index)
data_final1 = data_final1.drop(data_final[(data_final['PARKING_CITY'] == 'JAMMU') & (data_final['PICKUP_REGION_NAME'] == 'JAMMU AND KASHMIR')].index)
data_final1 = data_final1.drop(data_final[(data_final['PARKING_CITY'] == 'AMRAVATI') & (data_final['PICKUP_REGION_NAME'] == 'Z-AMRAVATI')].index)
ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/17ZEHIYeo9lKWvRP2-vKQ6UdeoY9Nnxwz8K9UhcPLNqI/edit#gid=1317334778').worksheet('filter_data')
# gd.set_with_dataframe(ws1,data_final1,resize=True,row=1,col=1)  #write
check=pd.DataFrame(ws1.get_all_records()) 
check=check[['Concat']]

check=[str(x) for x in check['Concat']]
check_t = tuple(check)
data_final1 = data_final1[~data_final1['LANE_CONCAT'].isin(check_t)]
data_final1=data_final1[~data_final1['GFD_STATUS'].isin(['GFD_CONFIRMED'])]


ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/17ZEHIYeo9lKWvRP2-vKQ6UdeoY9Nnxwz8K9UhcPLNqI/edit#gid=0').worksheet('Pendency')
gd.set_with_dataframe(ws,data_final1,resize=True,row=1,col=1)  #write

sheet_data=data_final1[['LEAD_ID','REGISTRATION_NUMBER','MAKE','MODEL','PARKING_CITY','LATEST_PARKING_YARD','SALE_CONFIRMED_DATE','PICKUP_REGION_NAME','GFD_STATUS','CURRENT_CAR_STATUS','LANE_CONCAT','REGISTRATION_YEAR','C24_QUOTE','AGING_YEAR_FROM_REGISTRATION_YEAR','FLAG','C2B/GS']]
print(sheet_data)
sheet_data[['GFD_STATUS','SALE_CONFIRMED_DATE','CURRENT_CAR_STATUS']]=sheet_data[['GFD_STATUS','SALE_CONFIRMED_DATE','CURRENT_CAR_STATUS']].replace('',np.nan)
sheet_data['GFD_STATUS']=sheet_data['GFD_STATUS'].replace(np.nan,'GFD_PENDING')
import datetime
today = datetime.datetime.now()

sheet_data['SALE_CONFIRMED_DATE'] = sheet_data['SALE_CONFIRMED_DATE'].fillna(today)
sheet_data['CURRENT_CAR_STATUS'] = sheet_data['CURRENT_CAR_STATUS'].replace(np.nan,'CHECKED_IN')
sheet_data['Mapping_Date'] = today_date
sheet_data['Transporter_Name']=''
sheet_data['Category']=''
sheet_data['FUEL_TYPE']=''
sheet_data['BID_CONFIRMED_DEALER_CODE']=''
sheet_data['BID_CONFIRMED_DEALER_NAME']=''
sheet_data=sheet_data[['LEAD_ID','REGISTRATION_NUMBER','MAKE','MODEL','PARKING_CITY','LATEST_PARKING_YARD','SALE_CONFIRMED_DATE','PICKUP_REGION_NAME','GFD_STATUS','CURRENT_CAR_STATUS','Mapping_Date','LANE_CONCAT','Transporter_Name','Category','C24_QUOTE','C2B/GS','FUEL_TYPE','REGISTRATION_YEAR','AGING_YEAR_FROM_REGISTRATION_YEAR','FLAG','BID_CONFIRMED_DEALER_CODE','BID_CONFIRMED_DEALER_NAME']]
ws11=gc.open_by_url('https://docs.google.com/spreadsheets/d/17ZEHIYeo9lKWvRP2-vKQ6UdeoY9Nnxwz8K9UhcPLNqI/edit#gid=0').worksheet('Master_data1')
# gd.set_with_dataframe(ws11,data_final1,resize=True,row=1,col=1)  #write
master_data=pd.DataFrame(ws11.get_all_records()) 

master_data=pd.concat([master_data,sheet_data],ignore_index=True)
ws12=gc.open_by_url('https://docs.google.com/spreadsheets/d/17ZEHIYeo9lKWvRP2-vKQ6UdeoY9Nnxwz8K9UhcPLNqI/edit#gid=0').worksheet('Master_data1')
gd.set_with_dataframe(ws12,master_data,resize=False,row=1,col=1)
#Backup_data
ws12=gc.open_by_url('https://docs.google.com/spreadsheets/d/1pT6CDU0cNoaO0cukmG8ZQOWw9N14sQLEFVHDuz8fjBA/edit#gid=0').worksheet('Master_data1')
gd.set_with_dataframe(ws12,master_data,resize=False,row=1,col=1)

ws5=gc.open_by_url('https://docs.google.com/spreadsheets/d/1NtK-88ydwNFP9F4Lznf-VPcwInXOhLuGG7y2AKMOOBA/edit#gid=1392538818').worksheet('Concat_data')
# gd.set_with_dataframe(ws,data_final1,resize=True,row=1,col=1)  #write
bd=pd.DataFrame(ws5.get_all_records()) 
bd.columns = bd.columns.astype(str).str.strip()

bd['C2B/GS']=''

ws5=gc.open_by_url('https://docs.google.com/spreadsheets/d/17ZEHIYeo9lKWvRP2-vKQ6UdeoY9Nnxwz8K9UhcPLNqI/edit#gid=1179011715').worksheet('Pendency')
# gd.set_with_dataframe(ws,data_final1,resize=True,row=1,col=1)  #write
data=pd.DataFrame(ws5.get_all_records()) 
data['In_Transit_date']=''
data['Stockin']=''
data['Mapping_Date']=today_date
data['SALE_CONFIRMED_DATE'] = data['SALE_CONFIRMED_DATE'].replace('',np.nan)
data['SALE_CONFIRMED_DATE'] = data['SALE_CONFIRMED_DATE'].fillna(today)
data['Mapping_Date'] = pd.to_datetime(data['Mapping_Date'], format='%Y-%m-%d').dt.strftime('%d-%m-%Y')
data=data[['LEAD_ID','REGISTRATION_NUMBER','MAKE','MODEL','PARKING_CITY','LATEST_PARKING_YARD','SALE_CONFIRMED_DATE','In_Transit_date','PICKUP_REGION_NAME','Stockin','Mapping_Date','LANE_CONCAT','C2B/GS']]
billing_data=pd.concat([bd,data],ignore_index=True)
billing_data['LANE_CONCAT']=billing_data['LANE_CONCAT'].replace('Bangalore','BENGALURU')
billing_data.columns = billing_data.columns.astype(str).str.strip()

if (billing_data['C2B/GS'] == 'GS').any():
    billing_data.loc[billing_data['C2B/GS'] == 'GS', 'LANE_CONCAT'] = billing_data.loc[billing_data['C2B/GS'] == 'GS', 'LANE_CONCAT'].str.replace('Bangalore', 'Bengaluru')
billing_data=billing_data[['LEAD_ID','REGISTRATION_NUMBER','MAKE','MODEL','PARKING_CITY','LATEST_PARKING_YARD','SALE_CONFIRMED_DATE','PICKUP_REGION_NAME','Mapping_Date','LANE_CONCAT','C2B/GS']]

ws6=gc.open_by_url('https://docs.google.com/spreadsheets/d/1NtK-88ydwNFP9F4Lznf-VPcwInXOhLuGG7y2AKMOOBA/edit#gid=1392538818').worksheet('Concat_data')
gd.set_with_dataframe(ws6,billing_data,resize=False,row=1,col=1)  #write
#PTL Backup
ws121=gc.open_by_url('https://docs.google.com/spreadsheets/d/1NtK-88ydwNFP9F4Lznf-VPcwInXOhLuGG7y2AKMOOBA/edit#gid=2107925430').worksheet('Raw Data')
# gd.set_with_dataframe(ws121,master_data,resize=False,row=1,col=1)
ptl_data=pd.DataFrame(ws121.get_all_records()) 

ws1212=gc.open_by_url('https://docs.google.com/spreadsheets/d/1pT6CDU0cNoaO0cukmG8ZQOWw9N14sQLEFVHDuz8fjBA/edit#gid=0').worksheet('PTL_Raw_data')
gd.set_with_dataframe(ws1212,ptl_data,resize=True,row=1,col=1)

data_final11=data_final1[['LEAD_ID','REGISTRATION_NUMBER','MAKE','MODEL','PARKING_CITY','LATEST_PARKING_YARD','SALE_CONFIRMED_DATE','PICKUP_REGION_NAME','LANE_CONCAT','FLAG']]
data_final11=data_final11[~data_final11['LANE_CONCAT'].isin(['Gurgaon-Delhi NCR','New Delhi-Delhi NCR','DELHI/NCR-GHAZIABAD CITY','UJJAIN-UJJAIN CITY'])]
html_data=data_final11.to_html(classes='center', index=False, escape=False, justify='center')
html_data = html_data.replace('<table', '<table style="text-align:center;"')

email_id=['vishal.singh@aigc.co.in','abhishek.shukla@cars24.com','varunkaushik@trfvlsl.com','chandra.shekhar@aigc.co.in','anilsharma@trfvlsl.com','gaurav.singh@aigc.co.in','shiv.yadav@cars24.com','operations.mum@cars24.com','operations.tru@cars24.com','operations.bho@cars24.com','operations.pun@cars24.com','operations.ncr@cars24.com','operations.cbe@cars24.com','operations.che@cars24.com','operations.up@cars24.com','operations.mad@cars24.com','operations.sal@cars24.com','trichy.operations@cars24.com','operations.hyd@cars24.com','operations.cdh@cars24.com',	'operations.ldh@cars24.com','ops.direct.punj@cars24.com','operations.ind@cars24.com','ops.direct.guj@cars24.com','operations.mhr@cars24.com','opeartions.mhr@cars24.com','ops.direct.mah@cars24.com']
html = 'Hi Vishal<br> <br> Please find below today Request <br><br>' +html_data +'<br> <br> Regards<br> Sahil '
subject="Interstate Transfer Request AIGC -" +today_date
# create_message_without_attachment(email_id,subject,html)
data_con = data_final1[data_final1['LANE_CONCAT'].isin(check_t)]



if data_con['LEAD_ID'].isnull().all():
    print("LEAD_ID is blank. Skipping HTML table creation.")
else:
    # Assuming selected_rows is your DataFrame
    html_table = data_con.to_html(index=False)
    
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
            <h3>PTS Pendency Report </h3>
        </div>
        <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
            <h4>PTS Pendency Report -{yesterday_date1}:</h4>
            {html_table}
        </div>
    
    </body>
    </html>
    """

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
        
        driver.set_window_size(1000, 720)
        
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




html_table = selected_rows.to_html(index=False)


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
        <h3>PTS Pendency Report</h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>PTS Pendency Report -{yesterday_date1}:</h4>
        {html_table}
    </div>
   
</body>
</html>
"""

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
    
    driver.set_window_size(1200, 720)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'test_report.html'
    png_file_path = 'test_report.png'

    html_to_png(html_file_path, png_file_path)


secret_key=os.environ['secret_key']
slack_token = secret_key
client = WebClient(token=slack_token)

channel = 'C06LN33R41F' 
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