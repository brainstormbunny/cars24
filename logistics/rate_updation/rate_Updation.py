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
import math
import zipfile
import os
import warnings
warnings.filterwarnings("ignore")
import numpy as npx
import gspread
import gspread_dataframe as gd

# variable = 'Vapi'
# Latur
# Pondicherry
# Vapi



email = 'sahil.5@cars24.com'
WAREHOUSE = 'BI_WH'
# # Connect to Snowflake
ctx = snowflake.connector.connect(
    user=email,
    warehouse=WAREHOUSE,
    account='am62076.ap-southeast-2',
    authenticator="externalbrowser"  # Assuming you're using external browser authentication
)
cur = ctx.cursor()



home_directory = 'C:/Users/Cars24/Desktop/Notebook/'
gsheet_auth = f'{home_directory}sahil_creds.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)


query = """

Select NAME as STORE_NAME,REGION_ID from "PC_STITCH_DB"."ADMIN_PANEL_PROD_DEALERENGINE_PROD"."REGIONS"
group by 1,2

        """
        
cur.execute(query)
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
df['STORE_NAME'] = df['STORE_NAME'].replace('Tiruchirappalli.', 'Tiruchirappalli')
ws2=gc.open_by_url('https://docs.google.com/spreadsheets/d/1jO76G5uehEJm2gmM7A8RyAwDc_xaJEQcWf5sDdp-_lY/edit#gid=0').worksheet('All_Data')
gd.set_with_dataframe(ws2,df,resize=True,row=1,col=1)  

df1=pd.read_csv('transportation_rate_data.csv')
df1=df1[['ORIGIN_CITY','DESTINATION_CITY','CATEGORY','TAT','New Proposed Freight Feb']]
df1=df1[~df1['ORIGIN_CITY'].isna()]
df1 = df1.loc[df1.groupby(['ORIGIN_CITY', 'DESTINATION_CITY', 'CATEGORY', 'TAT'])['New Proposed Freight Feb'].idxmax()]
x=['Amravati','Ambala']
for variable in x:
    oc=df1[df1['ORIGIN_CITY'].isin([variable])]
    dc=df1[df1['DESTINATION_CITY'].isin([variable])]
    ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1jO76G5uehEJm2gmM7A8RyAwDc_xaJEQcWf5sDdp-_lY/edit#gid=0').worksheet('Need_to_Update')
    # gd.set_with_dataframe(ws2,df,resize=True,row=1,col=1)  
    code=pd.DataFrame(ws1.get_all_records())

    mapp=code[['City','Origin_Code']]
    mapp1=mapp.rename(columns={'City':'ORIGIN_CITY'})
    data=oc.merge(mapp1,on='ORIGIN_CITY',how='left')
    data['New Proposed Freight Feb']=data['New Proposed Freight Feb'].replace(np.nan,0)
    data[['CATEGORY','TAT','New Proposed Freight Feb','Origin_Code']]=data[['CATEGORY','TAT','New Proposed Freight Feb','Origin_Code']].astype(int)
    data = data.drop_duplicates()
    mapp2=df[['STORE_NAME','REGION_ID']]
    mapp2=mapp2.rename(columns={'STORE_NAME':'ORIGIN_CITY','REGION_ID':'Origin_Code'})
    data1=dc.merge(mapp2,on='ORIGIN_CITY',how='left')
    data1 = data1.drop_duplicates()
    data1=dc.merge(mapp2,on='ORIGIN_CITY',how='left')
    data1 = data1.drop_duplicates()
    nb=data1[~data1['Origin_Code'].isna()]
    bl=data1[data1['Origin_Code'].isna()]
    ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1jO76G5uehEJm2gmM7A8RyAwDc_xaJEQcWf5sDdp-_lY/edit#gid=0').worksheet('Mapping')
    rep1=pd.DataFrame(ws1.get_all_records())
    rep1=rep1[['Old','New']]
    rep=rep1.rename(columns={'Old':'ORIGIN_CITY'})
    bl=bl.merge(rep,on='ORIGIN_CITY',how='left')
    bl['ORIGIN_CITY'] = bl['ORIGIN_CITY'][~bl['ORIGIN_CITY'].isin(['Ankleshwar', 'UP-ALL-groups', 'JammuandKashmir'])]
    bl.drop_duplicates()
    bl=bl[['ORIGIN_CITY','DESTINATION_CITY','CATEGORY','TAT','New Proposed Freight Feb','New']]
    bl=bl[~bl['ORIGIN_CITY'].isna()]
    mapp3=df[['STORE_NAME','REGION_ID']]
    mapp3=mapp3.rename(columns={'STORE_NAME':'DESTINATION_CITY','REGION_ID':'Origin_Code'})
    bl = bl.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    mapp3 = mapp3.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    bl=bl.merge(mapp3,on='DESTINATION_CITY',how='left')
    # bl=bl.merge(mapp3,on='DESTINATION_CITY',how='left')
    bl=bl[['New','DESTINATION_CITY','CATEGORY','TAT','New Proposed Freight Feb']]
    bl=bl.rename(columns={'New':'ORIGIN_CITY'})
    mapp3=mapp3.rename(columns={'DESTINATION_CITY':'ORIGIN_CITY'})
    bl=bl.merge(mapp3,on='ORIGIN_CITY',how='left')
    bl = bl.drop_duplicates()

    data1=pd.concat([nb,bl],ignore_index=False)
    data[['CATEGORY','TAT','New Proposed Freight Feb','Origin_Code']]=data[['CATEGORY','TAT','New Proposed Freight Feb','Origin_Code']].astype(int)
    data1=pd.concat([data1,data1],ignore_index=False)
    final_data=pd.concat([data,data1],ignore_index=False)
    mapp4=df[['STORE_NAME','REGION_ID']]
    mapp4=mapp4.rename(columns={'STORE_NAME':'DESTINATION_CITY','REGION_ID':'Destination_Code'})
    final_data['DESTINATION_CITY'] = final_data['DESTINATION_CITY'].replace('Tiruchirappalli.', 'Tiruchirappalli')
    final_data1=final_data.merge(mapp4,on='DESTINATION_CITY',how='left')
    final_data1['DESTINATION_CITY'] = final_data1['DESTINATION_CITY'][~final_data1['DESTINATION_CITY'].isin(['Ankleshwar', 'UP-ALL-groups', 'JammuandKashmir'])]
    final_datanb=final_data1[~final_data1['Destination_Code'].isna()]
    final_datab=final_data1[final_data1['Destination_Code'].isna()]
    final_datab=final_datab.rename(columns={'Old':'DESTINATION_CITY'})
    rep11=rep1.rename(columns={'Old':'DESTINATION_CITY'})
    final_datab=final_datab.merge(rep11,on='DESTINATION_CITY',how='left')
    final_datab=final_datab.drop_duplicates()
    mapp5=df[['STORE_NAME','REGION_ID']]
    mapp5=mapp5.rename(columns={'STORE_NAME':'New','REGION_ID':'Destination_Code'})
    final_datab=final_datab[['ORIGIN_CITY','DESTINATION_CITY','CATEGORY','TAT','New Proposed Freight Feb','Origin_Code','New']]

    final_datab=final_datab.merge(mapp5,on='New',how='left')
    datafinal=pd.concat([final_datanb,final_datab],ignore_index=False)
    mapping_dict = {1: 'Category_1', 2: 'Category_2', 3: 'Category_3', 4: 'Category_4', 5: 'Category_5'}

    datafinal['CATEGORY'] = datafinal['CATEGORY'].replace(mapping_dict)
    datafinal=datafinal[~datafinal['Destination_Code'].isna()]

    datafinal=datafinal.rename(columns={'Origin_Code':'origin','Destination_Code':'destination','CATEGORY':'vehicle_type','New Proposed Freight Feb':'cost','TAT':'tat'})

    datafinal=datafinal[['ORIGIN_CITY', 'DESTINATION_CITY', 'vehicle_type', 'tat', 'origin', 'destination','cost']]


    datafinal_grouped1 = datafinal.loc[datafinal.groupby(['ORIGIN_CITY', 'DESTINATION_CITY', 'vehicle_type', 'origin', 'destination'])['cost'].idxmax()]

    datafinal_grouped1=datafinal_grouped1[['origin','destination','vehicle_type','cost','tat']]


    # Define the chunk size and variable
    chunk_size = 1999

    # Calculate the number of chunks needed
    num_chunks = math.ceil(len(datafinal_grouped1) / chunk_size)

    # Create a zip file to store all the CSV files
    zip_file_path = f'C:/Users/Cars24/Desktop/Rate_updation/{variable}_files.zip'
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        # Iterate over chunks of the DataFrame and save each chunk to a separate CSV file
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(datafinal_grouped1))
            chunk_df = datafinal_grouped1.iloc[start_idx:end_idx]

            # Save the chunk to a CSV file
            chunk_csv_path = f'C:/Users/Cars24/Desktop/Rate_updation/{variable}{i+1}.csv'
            chunk_df.to_csv(chunk_csv_path, index=False)

            # Add the CSV file to the zip file
            zipf.write(chunk_csv_path, os.path.basename(chunk_csv_path))

    for i in range(num_chunks):
        chunk_csv_path = f'C:/Users/Cars24/Desktop/Rate_updation/{variable}{i+1}.csv'
        os.remove(chunk_csv_path)

    print("CSV files have been saved and zipped successfully.")
    zip_file_path = f'C:/Users/Cars24/Desktop/Rate_updation/{variable}_files.zip'
    from gmail_functions import create_message_with_attachment

    email_id=['abhishek.shukla@cars24.com']
    html = 'Hi Aman/Anil <br> <br> Please update below freight charges and please confirm once it done, <br><br> Regards<br> Sahil'
    subject = "Rate Updation for " + variable
    create_message_with_attachment(email_id,subject,html,zip_file_path)
