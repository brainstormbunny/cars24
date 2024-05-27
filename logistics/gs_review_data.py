import snowflake.connector
import gspread
import pandas as pd
import gspread_dataframe as gd
import time
import winsound
import os
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
import datetime as dt
import sys
import time as tm
import numpy as np
import warnings
warnings.filterwarnings("ignore")
import numpy as npx
import gspread
import gspread_dataframe as gd
gsheet_auth='sahil_creds.json'

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)

def chunked(iterable, size):
    return [iterable[i:i+size] for i in range(0, len(iterable), size)]
x=['EWB','SKA','NHR','NDL','NUK','NPB','PUN','CHN','STN','UPW','WGJ','UPE','MUM-May 24','STS FEB 24','SAP FEB 24','NRJ May24','EOR  2024','WMH MAY 24','WMP-May24','SKL','EOR  2024']
# y='STS FEB 24','SAP FEB 24',"NRJ May'24",'EOR  2024','WMH MAY 24','WMP-May24'
all_data=pd.DataFrame()
for i in x:
    sheet_url = 'https://docs.google.com/spreadsheets/d/1J0JTlYSWeakSTPvE7Hsw99AzQnK2c_udWU-1-JT2rbU/edit#gid=1467218077'
    sheet = gc.open_by_url(sheet_url)
    worksheet = sheet.worksheet(i)
    cell_range1 = worksheet.range("A1:K")
    data = [[cell.value for cell in row] for row in chunked(cell_range1, 11)]
    data1 = pd.DataFrame(data)
    data1.columns = data1.iloc[0]
    data1 = data1.drop(data1.index[0]).reset_index(drop=True)
    data1=data1.replace(np.nan,'')
    data1=data1[['Date','App_ID','Vehicle_Number','Customer_Name','City','Driver_Name','Driver_Locus_ID','Region','Month']]
    data1=data1.replace('',np.nan)
    data1=data1[~data1['Date'].isna()]
    data1=data1[data1['Month']=='May']
    all_data=pd.concat([all_data,data1],ignore_index=True)

def parse_date(date_str):
    formats = ['%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y','%d/%m/%Y %H:%M:%S']  # Add more formats as needed
    for fmt in formats:
        try:
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            pass
    return None  # Return None if no format matches

all_data['Date'] = all_data['Date'].apply(parse_date)


ws1=gc.open_by_url('https://docs.google.com/spreadsheets/d/1fo8q9ivgLykEcyeSbZwVfBznjq39yc3qkoDjEhua4mI/edit#gid=1425054139').worksheet('Compiled Data')

gd.set_with_dataframe(ws1,all_data,resize=False,row=1,col=1)
print('Done')
sys.exit()

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

query = """
Select LEAD_ID,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,PICKUP_DATE_FINAL,FIRST_STOCKIN_DATE,
STORE_TYPE,DEALER_CODE,GS_NON_GS_FLAG,TRACKER_LAST_UPDATED_AT,make,model,variant from 
(Select T4.LEAD_ID,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,PICKUP_CREATED,FIRST_STOCKIN_DATE,PICKUP_DATE_SYSTEM,PICKUP_DATE_FINAL,
PICKUP_TIME_SLOT_TO,STOCKIN_DATE,STORE_TYPE,PAYMENT_TYPE,PAYMENT_DATE,STATUS_PICKUP,ASSIGNED_AT,COMPLETE_ON,RESCHEDULED_AT,CONDITION,IS_PICKED,
IN_TRANSIT_AT,LATEST_INSPECTION_DATE,CUSTOMER_PICKUP_INTENT,FIRST_INSPECTION_DATE,DRIVER_TYPE,DEALER_CODE,Tracker_Last_Updated_At,GS_NON_GS_FLAG,IS_BREACH,
SLOT,OPT_SENT,OTP_VERIFIED_AT,OTP_SENT_DATE,IS_BREACH_FLAG,IS_PLL_CASE,make,model,variant
 from 
(Select *,
Case when least(ifnull(ifnull(PAYMENT_DATE,OTP_SENT_DATE),FIRST_STOCKIN_DATE),
ifnull(ifnull(OTP_SENT_DATE,FIRST_STOCKIN_DATE),PAYMENT_DATE),
ifnull(ifnull(FIRST_STOCKIN_DATE,PAYMENT_DATE),OTP_SENT_DATE)) > PICKUP_DATE_FINAL then 1 else 0 end  as IS_Breach,
Case when 
least(ifnull(ifnull(PAYMENT_DATE,OTP_SENT_DATE),FIRST_STOCKIN_DATE),
ifnull(ifnull(OTP_SENT_DATE,FIRST_STOCKIN_DATE),PAYMENT_DATE),
ifnull(ifnull(FIRST_STOCKIN_DATE,PAYMENT_DATE),OTP_SENT_DATE)) > ifnull(Customer_Pickup_Intent,PICKUP_DATE_FINAL) then 1 
when (first_stockin_date is null and payment_date is null and otp_sent_date is null and Customer_Pickup_Intent < (sysdate()+interval '330 minute')) then 1
when (first_stockin_date is null and STORE_TYPE = 'HI' AND OPT_SENT IS NULL AND Customer_Pickup_Intent < (sysdate()+interval '330 minute')) then 1

else 0 end  as IS_Breach_Flag
 from 
(Select LEAD_ID,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,PICKUP_CREATED,FIRST_STOCKIN_DATE,make,model,variant,
PICKUP_DATE_SYSTEM,PICKUP_DATE_FINAL,to_char(PICKUP_TIME_SLOT_TO) PICKUP_TIME_SLOT_TO--,BREACH_COUNT,HOURS_BUCKET
,STOCKIN_DATE,
STORE_TYPE,PAYMENT_TYPE,PAYMENT_DATE,STATUS_PICKUP,ASSIGNED_AT,COMPLETE_ON--,IS_RESCHEDULED
,RESCHEDULED_AT,
CONDITION,IS_PICKED--,CANCELLED_AT
,IN_TRANSIT_AT,INSPECTION_DATE AS LATEST_INSPECTION_DATE,Customer_Pickup_Intent,
to_timestamp(FI.INSPECTIONSTARTTIME_F) AS FIRST_INSPECTION_DATE,DRIVER_TYPE,dealer_code,dealer_name,Tracker_Last_Updated_At,
case when dealer_code in ('63645','83137','75812','83645','109842') then 'GS' else 'NON_GS' end as GS_NON_GS_FLAG,
concat(concat(PICKUP_TIME_SLOT_FROM,'-'),PICKUP_TIME_SLOT_TO) as Slot,
OTP.CREATED_AT AS OPT_Sent,
to_timestamp(OTP.VERIFIED_AT) AS OTP_VERIFIED_AT,
IFNULL(OPT_SENT,PAYMENT_DATE) AS OTP_Sent_Date,

Case when STORE_TYPE = 'PnS' and Diff >4320 then 1
      when STORE_TYPE = 'PnS' and Diff <=4320 then 0
      when UPPER(STORE_TYPE) = 'HI' and Diff >720 then 1
      when UPPER(STORE_TYPE) = 'HI' and Diff <=720 then 0
	  when Diff > 2160 then 1 else 0 end as BREACH_COUNT,
Case when Diff <= 720 then '12'
	 when Diff <= 2160 then '36'
	 when Diff <= 2880 then '48'
	 when Diff <= 4320 then '72'
	 when Diff <= 10080 then '7 days' else '7 days above' end as HOURS_BUCKET
 from 
(Select *
--,Case when to_date(PICKUP_DATE_FINAL) > CURRENT_DATE THEN 1 ELSE 0 END AS IS_Future_Pickup
,Case when to_date(STOCKIN_DATE) is not null then 1 ELSE 0 END AS IS_Picked,
pickup_date pickup_date_final,

Case When UPPER(STORE_TYPE) IN ('RETAIL','PNS') THEN DATEDIFF('Minutes',GREATEST(Payment_Date,PICKUP_CREATED),FIRST_STOCKIN_DATE)
--Case When UPPER(STORE_TYPE) IN ('RETAIL','PNS') THEN DATEDIFF('Minutes',ifnull(Payment_Date,pickup_date),FIRST_STOCKIN_DATE)
     When UPPER(STORE_TYPE) = 'HI' AND HOUR(pickup_date) > 16 
	 THEN DATEDIFF('Minutes',DATEADD(DAY,1,pickup_date),FIRST_STOCKIN_DATE)
     else DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) end as Diff,
DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) Diff_Old,	 
case when DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) > 2160 then 1 else 0 end as breach_count_old

 from (Select LEAD_ID,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,make,model,variant
,PICKUP_CREATED,FIRST_STOCKIN_DATE
,PICKUP_DATE_PICKUP as pickup_date_system
,Customer_Pickup_Intent
,to_char(PICKUP_TIME_SLOT_FROM) as PICKUP_TIME_SLOT_FROM
,to_char(PICKUP_TIME_SLOT_TO) as PICKUP_TIME_SLOT_TO
,STOCKIN_DATE,STORE_TYPE,PAYMENTHEADING as PAYMENT_TYPE,PAYMENT_DATE,STATUS_PICKUP
,CANCELLED_AT,IN_TRANSIT_AT,ASSIGNED_AT,
COMPLETE_ON,IS_RESCHEDULED, RESCHEDULED_AT,Condition,INSPECTION_DATE,DRIVER_TYPE
,dealer_code,dealer_name,Tracker_Last_Updated_At,

Case when STORE_TYPE in ('HI') and PICKUP_TIME_SLOT_TO is null then to_timestamp(PICKUP_DATE2)
	 when STORE_TYPE in ('HI') then concat(concat(to_date(PICKUP_DATE2),' '),PICKUP_TIME_SLOT_TO) else to_timestamp(PICKUP_CREATED) end as pickup_date
	 
--,PAYMENTHEADING
--,Diff
from
(Select LEAD_ID,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,
PICKUP_CREATED,FIRST_STOCKIN_DATE,PICKUP_DATE_PICKUP,
Customer_Pickup_Intent,
PICKUP_TIME_SLOT_FROM,PICKUP_TIME_SLOT_TO,make,model,variant,
--Case when UPPER(STORE_TYPE2) = 'HI' AND DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) <= 720 then '12'
--when DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) <= 2160 then '36'
--when DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) <= 2880 then '48'
--when DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) <= 4320 then '72'
--when DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) <= 10080 then '7 days' else '7 days above' end as hours_bucket,

to_date(FIRST_STOCKIN_DATE) as stockin_Date,
STORE_TYPE2 AS STORE_TYPE,PAYMENTHEADING,Payment_Date,STATUS_PICKUP,CANCELLED_AT,IN_TRANSIT_AT,ASSIGNED_AT,
COMPLETE_ON,IS_RESCHEDULED, RESCHEDULED_AT,Condition,INSPECTION_DATE,DRIVER_TYPE
,dealer_code,dealer_name
,cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ) Tracker_Last_Updated_At,
PICKUP_DATE2
from (SELECT ORDERS.LEAD_ID,P.PARKING,ORDERS.make,ORDERS.model,ORDERS.variant,
--P.REGION as Parking_region,Cl.centre,
Case when UPPER(P.PARKING) like '%BANGALORE%' THEN 'BLR' ELSE P.REGION END as Parking_region,Cl.centre,
CL.REGION AS RETAIL_REGION,CL.CITY AS RETAIL_CITY,
to_timestamp(OPS.CREATED_AT) AS PICKUP_CREATED,to_timestamp(OLA.CREATED_AT) AS FIRST_STOCKIN_DATE,to_timestamp(OPS.PICKUP_DATE) AS PICKUP_DATE_PICKUP,PICKUP_TIME_SLOT_FROM,PICKUP_TIME_SLOT_TO,STORE_TYPE,
Case when (OPS.PICKUP_DATE is null AND OPS.STATUS = '0'and OPS.CANCELLED_AT is not null) then to_timestamp(OPS.CUSTOMER_PICKUP_DATE) else to_timestamp(OPS.PICKUP_DATE) end as PICKUP_DATE2,
	 
Case when STORE_TYPE2 in ('HI') and CUSTOMER_PICKUP_TIME_SLOT_TO is null then to_timestamp(OPS.CUSTOMER_PICKUP_DATE)
	 when STORE_TYPE2 in ('HI') then concat(concat(to_date(OPS.CUSTOMER_PICKUP_DATE),' '),CUSTOMER_PICKUP_TIME_SLOT_TO) else to_timestamp(OPS.CREATED_AT) end as Customer_Pickup_Intent
,STORE_TYPE2,PM.PAYMENTHEADING,PM.Payment_Date
,case when OPS.STATUS = '0' then 'Pending'
	  when OPS.STATUS = '1' then 'Assigned'
	  when OPS.STATUS = '2' then 'Intransit'
	  when OPS.STATUS = '3' then 'Cancelled'
	  when OPS.STATUS = '-1' then 'Cancelled'
	  when OPS.STATUS = '4' then 'Completed' else null end AS STATUS_PICKUP
,TO_TIMESTAMP(CANCELLED_AT) AS CANCELLED_AT,TO_TIMESTAMP(OPS.IN_TRANSIT_AT) AS IN_TRANSIT_AT,TO_TIMESTAMP(OPS.ASSIGNED_AT) AS ASSIGNED_AT
,TO_TIMESTAMP(OPS.COMPLETE_ON) AS COMPLETE_ON, OPS.IS_RESCHEDULED, TO_TIMESTAMP(OPS.RESCHEDULED_AT) AS RESCHEDULED_AT
,Case when ORDERS.IS_SCRAP = 'NO' and OPS.VEHICLE_CONDITION = 'perfect' then 'Non Scrap & Perfect Working Condition Car' else 'Scrap & Non-Working Condition Car' end as Condition
,TO_TIMESTAMP(ORDERS.LAST_INSPECTION_DATE) AS INSPECTION_DATE,
Case when OPS.DRIVER_TYPE = 1 then 'Inhouse'
	 when OPS.DRIVER_TYPE = 2 then 'Drive U'
	 when OPS.DRIVER_TYPE in (0,4,5,6,9) then 'Others'
 	 when OPS.DRIVER_TYPE = 7 then 'Soos'
	 when OPS.DRIVER_TYPE = 8 then '21 North'
	 when OPS.DRIVER_TYPE = 10 then 'Breakdown - Towing'
	 when OPS.DRIVER_TYPE is null then 'Allocation missed but picked up' end as DRIVER_TYPE
,s.dealer_code,s.dealer_name
	 
FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS
LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_PICKUP_SERVICE OPS ON OPS.FK_ORDER_ID = ORDERS.ORDER_ID AND OPS.SERVICE_TYPE = 'PICKUP'
Left JOIN (SELECT * FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY FK_ORDER_ID ORDER by ORDER_INVENTORY_LOG_ID ASC) AS rank FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_INVENTORY_LOG WHERE ACTION = 'CHECKED_IN')A WHERE rank = 1)OLA ON OLA.FK_ORDER_ID = ORDERS.ORDER_ID
LEFT JOIN (Select *,Case when STORE_TYPE = 'PnS' then 'PnS'when LEFT(UPPER(CENTRE),3) = 'PLL' then 'PLL'
						 when STORE_TYPE = 'locality' then 'HI'
						 when STORE_TYPE = 'B2B' then 'B2B' else 'Retail' end as STORE_TYPE2 from PC_STITCH_DB.FIVETRAN1_BI.CENTRE_LIST) CL ON ORDERS.STORE_ID = CL.CENTRE_ID
LEFT JOIN PC_STITCH_DB.FIVETRAN1_BI.PARKING P ON P.LOCATION_ID = OLA.FK_LOCATION_ID_TO
LEFT JOIN (SELECT A.* FROM (SELECT LEAD_ID,PAYMENTHEADING,CREATED_AT  as Payment_Date,ROW_NUMBER() OVER (PARTITION BY LEAD_ID ORDER BY PAYMENTHEADING ASC) as rank
FROM (SELECT LEAD_ID, CASE WHEN UPPER(PAYMENT_HEADING) IN ('DELIVERY','LOAN') THEN 'DELIVERY'
						   WHEN UPPER(PAYMENT_HEADING) IN ('TOKEN') THEN 'TOKEN' ELSE NULL END AS PAYMENTHEADING,TO_TIMESTAMP(OBI.CREATED_AT) AS CREATED_AT
FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_BANK_INSTRUCTION OBI
LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_PAYMENT_INSTRUCTION OPI ON OPI.ORDER_PAYMENT_INSTRUCTION_ID = OBI.FK_PAYMENT_INSTRUCTION
LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_PURCHASE_REQUEST OPR ON OPR.ORDER_PURCHASE_REQUEST_ID = OPI.FK_PURCHASE_REQUEST
LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS ON ORDERS.ORDER_ID = OPR.FK_ORDER_ID
WHERE OPI.STATUS = 3 AND OBI.STATUS = 3 AND UPPER(PAYMENT_HEADING) IN ('DELIVERY','LOAN','TOKEN') ORDER BY PAYMENTHEADING ASC)A) A 
WHERE rank = 1)PM ON to_char(PM.LEAD_ID) = to_char(ORDERS.LEAD_ID)
LEFT JOIN PC_STITCH_DB.FIVETRAN1_BI.SALES_TRANSACTIONS S ON S.APPT_ID = ORDERS.LEAD_ID


WHERE 
--TO_DATE(OLA.CREATED_AT) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-61 AND 
ORDERS.STATUS_ID <> 12 AND ORDERS.IS_DEAL_LOST_REQUEST = 0 AND OPS.CREATED_AT IS NOT NULL AND UPPER(CL.CENTRE) NOT LIKE ('%B2B%')
--AND UPPER(Cl.centre) NOT LIKE '%-PNS%' AND UPPER(Cl.centre) NOT LIKE '%- PNS%'
ORDER BY OLA.CREATED_AT ASC))T)T2
Where TO_DATE(first_stockin_date) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-65)T2
LEFT JOIN (Select APPOINTMENTID,INSPECTIONSTARTTIME_F from PC_STITCH_DB.WORK.FIRST_INSPECTION_DATA) FI ON T2.LEAD_ID = FI.APPOINTMENTID
LEFT JOIN 
(Select * from (Select *,
ROW_NUMBER() OVER(PARTITION BY APP_ID ORDER BY LAST_OTP_SENT_DATE)as Rnk
 from (SELECT split_part(REFERENCE_NO,'_',1) as App_ID,*
 from "PC_STITCH_DB"."ADMIN_PANEL_PROD_DEALERENGINE_PROD"."CUSTOMER_OTP")) WHERE Rnk = 1) OTP ON T2.LEAD_ID = OTP.APP_ID)T3)T4

LEFT JOIN (Select ORDERS.LEAD_ID,ORDERS.ORDER_ID,OPR2.IS_PROJECT_LOST_LEAD,
CASE WHEN IS_PROJECT_LOST_LEAD = 1 THEN 1 ELSE 0 END AS IS_PLL_CASE
from PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS
LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_PURCHASE_REQUEST OPR2 ON OPR2.FK_ORDER_ID = ORDERS.ORDER_ID AND OPR2.STATUS = 3)PLL
ON T4.LEAD_ID = PLL.LEAD_ID)FT where STORE_TYPE in ('HI')
        """
        
cur.execute(query)
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

ws2=gc.open_by_url('https://docs.google.com/spreadsheets/d/1fo8q9ivgLykEcyeSbZwVfBznjq39yc3qkoDjEhua4mI/edit#gid=69874696').worksheet('SI')
gd.set_with_dataframe(ws2,df,resize=False,row=1,col=1)  