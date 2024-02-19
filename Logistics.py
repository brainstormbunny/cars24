# import snowflake.connector
import gspread
import pandas as pd
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
import warnings
warnings.filterwarnings("ignore")
import os

# email = 'sahil.5@cars24.com'
# WAREHOUSE = 'BI_WH'

# # Connect to Snowflake
# ctx = snowflake.connector.connect(
#     user=email,
#     warehouse=WAREHOUSE,
#     account='am62076.ap-southeast-2',
#     authenticator="externalbrowser"  # Assuming you're using external browser authentication
# )

cur = ctx.cursor()
# home_directory = 'C:/Users/Cars24/Desktop/Notebook/'
# gsheet_auth = f'{home_directory}sahil_creds.json'

gsheet_auth=os.environ.get('SAHIL_SA')
gsheet_auth='sahilapi@sahil-374614.iam.gserviceaccount.com'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)

# query = """Select LEAD_ID,REGISTRATION_NUMBER,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,PICKUP_CREATED,FIRST_STOCKIN_DATE,PICKUP_DATE_SYSTEM,
# PICKUP_DATE_FINAL,STOCKIN_DATE,STORE_TYPE,PAYMENT_TYPE,PAYMENT_DATE,STATUS_PICKUP,RESCHEDULED_AT,CONDITION,IS_PICKED,LATEST_INSPECTION_DATE,
# CUSTOMER_PICKUP_INTENT,OPT_SENT,OTP_SENT_DATE,OTP_VERIFIED_AT from (Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,
# CENTRE,RETAIL_REGION,RETAIL_CITY,REPLACE(PICKUP_CREATED,'.000','') AS PICKUP_CREATED,
# REPLACE(FIRST_STOCKIN_DATE,'.000','') AS FIRST_STOCKIN_DATE,REPLACE(PICKUP_DATE_SYSTEM,'.000','') AS PICKUP_DATE_SYSTEM,
# REPLACE(PICKUP_DATE_FINAL,'.000','') AS PICKUP_DATE_FINAL,
# --PICKUP_TIME_SLOT_TO
# RESCHEDULING_REASON
# ,STOCKIN_DATE,STORE_TYPE,PAYMENT_TYPE,
# REPLACE(PAYMENT_DATE,'.000','') AS PAYMENT_DATE,STATUS_PICKUP,IS_DEAL_LOST,
# --REPLACE(ASSIGNED_AT,'.000','') AS ASSIGNED_AT,
# REPLACE(COMPLETE_ON,'.000','') AS COMPLETE_ON,REPLACE(RESCHEDULED_AT,'.000','') AS RESCHEDULED_AT,CONDITION,IS_PICKED,
# --IN_TRANSIT_AT
# TASK_ID,
# REPLACE(LATEST_INSPECTION_DATE,'.000','') AS LATEST_INSPECTION_DATE,REPLACE(CUSTOMER_PICKUP_INTENT,'.000','') AS CUSTOMER_PICKUP_INTENT,
# REPLACE(FIRST_INSPECTION_DATE,'.000','') AS FIRST_INSPECTION_DATE,DRIVER_TYPE,DEALER_CODE,REPLACE(TRACKER_LAST_UPDATED_AT,'.000','') AS TRACKER_LAST_UPDATED_AT,
# GS_NON_GS_FLAG,IS_BREACH,SLOT,REPLACE(OPT_SENT,'.000','') AS OPT_SENT,REPLACE(OTP_VERIFIED_AT,'.000','') AS OTP_VERIFIED_AT,
# REPLACE(OTP_SENT_DATE,'.000','') AS OTP_SENT_DATE,IS_BREACH_FLAG,IS_PLL_CASE

#  from 
# (Select T4.LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,PICKUP_CREATED,FIRST_STOCKIN_DATE,PICKUP_DATE_SYSTEM,PICKUP_DATE_FINAL,
# PICKUP_TIME_SLOT_TO,STOCKIN_DATE,STORE_TYPE,PAYMENT_TYPE,PAYMENT_DATE,STATUS_PICKUP,ASSIGNED_AT,COMPLETE_ON,RESCHEDULED_AT,CONDITION,IS_PICKED,
# IN_TRANSIT_AT,LATEST_INSPECTION_DATE,CUSTOMER_PICKUP_INTENT,FIRST_INSPECTION_DATE,DRIVER_TYPE,DEALER_CODE,Tracker_Last_Updated_At,GS_NON_GS_FLAG,IS_BREACH,
# SLOT,OPT_SENT,OTP_VERIFIED_AT,OTP_SENT_DATE,IS_BREACH_FLAG,IS_PLL_CASE,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST
#  from 
# (Select *,
# Case when least(ifnull(ifnull(PAYMENT_DATE,OTP_SENT_DATE),FIRST_STOCKIN_DATE),
# ifnull(ifnull(OTP_SENT_DATE,FIRST_STOCKIN_DATE),PAYMENT_DATE),
# ifnull(ifnull(FIRST_STOCKIN_DATE,PAYMENT_DATE),OTP_SENT_DATE)) > PICKUP_DATE_FINAL then 1 else 0 end  as IS_Breach,
# Case when 
# least(ifnull(ifnull(PAYMENT_DATE,OTP_SENT_DATE),FIRST_STOCKIN_DATE),
# ifnull(ifnull(OTP_SENT_DATE,FIRST_STOCKIN_DATE),PAYMENT_DATE),
# ifnull(ifnull(FIRST_STOCKIN_DATE,PAYMENT_DATE),OTP_SENT_DATE)) > ifnull(Customer_Pickup_Intent,PICKUP_DATE_FINAL) then 1 
# when (first_stockin_date is null and payment_date is null and otp_sent_date is null and Customer_Pickup_Intent < (sysdate()+interval '330 minute')) then 1
# when (first_stockin_date is null and STORE_TYPE = 'HI' AND OPT_SENT IS NULL AND Customer_Pickup_Intent < (sysdate()+interval '330 minute')) then 1

# else 0 end  as IS_Breach_Flag
#  from 
# (Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,PICKUP_CREATED,FIRST_STOCKIN_DATE,
# PICKUP_DATE_SYSTEM,PICKUP_DATE_FINAL,to_char(PICKUP_TIME_SLOT_TO) PICKUP_TIME_SLOT_TO--,BREACH_COUNT,HOURS_BUCKET
# ,STOCKIN_DATE,
# STORE_TYPE,PAYMENT_TYPE,PAYMENT_DATE,STATUS_PICKUP,ASSIGNED_AT,COMPLETE_ON--,IS_RESCHEDULED
# ,RESCHEDULED_AT,
# CONDITION,IS_PICKED--,CANCELLED_AT
# ,IN_TRANSIT_AT,INSPECTION_DATE AS LATEST_INSPECTION_DATE,Customer_Pickup_Intent,
# to_timestamp(FI.INSPECTIONSTARTTIME_F) AS FIRST_INSPECTION_DATE,DRIVER_TYPE,dealer_code,dealer_name,Tracker_Last_Updated_At,
# case when dealer_code in ('63645','83137','75812','83645','109842') then 'GS' else 'NON_GS' end as GS_NON_GS_FLAG,
# concat(concat(PICKUP_TIME_SLOT_FROM,'-'),PICKUP_TIME_SLOT_TO) as Slot,
# OTP.CREATED_AT AS OPT_Sent,
# to_timestamp(OTP.VERIFIED_AT) AS OTP_VERIFIED_AT,
# IFNULL(OPT_SENT,PAYMENT_DATE) AS OTP_Sent_Date,

# Case when STORE_TYPE = 'PnS' and Diff >4320 then 1
#       when STORE_TYPE = 'PnS' and Diff <=4320 then 0
#       when UPPER(STORE_TYPE) = 'HI' and Diff >720 then 1
#       when UPPER(STORE_TYPE) = 'HI' and Diff <=720 then 0
# 	  when Diff > 2160 then 1 else 0 end as BREACH_COUNT,
# Case when Diff <= 720 then '12'
# 	 when Diff <= 2160 then '36'
# 	 when Diff <= 2880 then '48'
# 	 when Diff <= 4320 then '72'
# 	 when Diff <= 10080 then '7 days' else '7 days above' end as HOURS_BUCKET
# ,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST
#  from 
# (Select *
# --,Case when to_date(PICKUP_DATE_FINAL) > CURRENT_DATE THEN 1 ELSE 0 END AS IS_Future_Pickup
# ,Case when to_date(STOCKIN_DATE) is not null then 1 ELSE 0 END AS IS_Picked,
# pickup_date pickup_date_final,

# Case When UPPER(STORE_TYPE) IN ('RETAIL','PNS') THEN DATEDIFF('Minutes',GREATEST(Payment_Date,PICKUP_CREATED),FIRST_STOCKIN_DATE)
# --Case When UPPER(STORE_TYPE) IN ('RETAIL','PNS') THEN DATEDIFF('Minutes',ifnull(Payment_Date,pickup_date),FIRST_STOCKIN_DATE)
#      When UPPER(STORE_TYPE) = 'HI' AND HOUR(pickup_date) > 16 
# 	 THEN DATEDIFF('Minutes',DATEADD(DAY,1,pickup_date),FIRST_STOCKIN_DATE)
#      else DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) end as Diff,
# DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) Diff_Old,	 
# case when DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) > 2160 then 1 else 0 end as breach_count_old

#  from (Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY
# ,PICKUP_CREATED,FIRST_STOCKIN_DATE
# ,PICKUP_DATE_PICKUP as pickup_date_system
# ,Customer_Pickup_Intent
# ,to_char(PICKUP_TIME_SLOT_FROM) as PICKUP_TIME_SLOT_FROM
# ,to_char(PICKUP_TIME_SLOT_TO) as PICKUP_TIME_SLOT_TO
# ,STOCKIN_DATE,STORE_TYPE,PAYMENTHEADING as PAYMENT_TYPE,PAYMENT_DATE,STATUS_PICKUP
# ,CANCELLED_AT,IN_TRANSIT_AT,ASSIGNED_AT,
# COMPLETE_ON,IS_RESCHEDULED, RESCHEDULED_AT,Condition,INSPECTION_DATE,DRIVER_TYPE
# ,dealer_code,dealer_name,Tracker_Last_Updated_At,

# Case when STORE_TYPE in ('HI') and PICKUP_TIME_SLOT_TO is null then to_timestamp(PICKUP_DATE2)
# 	 when STORE_TYPE in ('HI') then concat(concat(to_date(PICKUP_DATE2),' '),PICKUP_TIME_SLOT_TO) else to_timestamp(PICKUP_CREATED) end as pickup_date
# ,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST
# --,PAYMENTHEADING
# --,Diff
# from
# (Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,
# PICKUP_CREATED,FIRST_STOCKIN_DATE,PICKUP_DATE_PICKUP,
# Customer_Pickup_Intent,
# PICKUP_TIME_SLOT_FROM,PICKUP_TIME_SLOT_TO,
# --Case when UPPER(STORE_TYPE2) = 'HI' AND DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) <= 720 then '12'
# --when DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) <= 2160 then '36'
# --when DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) <= 2880 then '48'
# --when DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) <= 4320 then '72'
# --when DATEDIFF('Minutes',pickup_date,FIRST_STOCKIN_DATE) <= 10080 then '7 days' else '7 days above' end as hours_bucket,

# to_date(FIRST_STOCKIN_DATE) as stockin_Date,
# STORE_TYPE2 AS STORE_TYPE,PAYMENTHEADING,Payment_Date,STATUS_PICKUP,CANCELLED_AT,IN_TRANSIT_AT,ASSIGNED_AT,
# COMPLETE_ON,IS_RESCHEDULED, RESCHEDULED_AT,Condition,INSPECTION_DATE,DRIVER_TYPE
# ,dealer_code,dealer_name
# ,cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ) Tracker_Last_Updated_At,
# PICKUP_DATE2,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST
# from (SELECT ORDERS.LEAD_ID,ORDERS.REGISTRATION_NUMBER,ORDERS.FUEL_TYPE,P.PARKING,
# --P.REGION as Parking_region,Cl.centre,
# Case when UPPER(P.PARKING) like '%BANGALORE%' THEN 'BLR' ELSE P.REGION END as Parking_region,IFNULL(mu.store_name,Cl.centre) AS centre,
# CL.REGION AS RETAIL_REGION,CL.CITY AS RETAIL_CITY,
# to_timestamp(OPS.CREATED_AT) AS PICKUP_CREATED,to_timestamp(OLA.CREATED_AT) AS FIRST_STOCKIN_DATE,to_timestamp(OPS.PICKUP_DATE) AS PICKUP_DATE_PICKUP,PICKUP_TIME_SLOT_FROM,PICKUP_TIME_SLOT_TO,STORE_TYPE,
# Case when (OPS.PICKUP_DATE is null AND OPS.STATUS = '0'and OPS.CANCELLED_AT is not null) then to_timestamp(OPS.CUSTOMER_PICKUP_DATE) else to_timestamp(OPS.PICKUP_DATE) end as PICKUP_DATE2,
	 
# Case when STORE_TYPE2 in ('HI') and CUSTOMER_PICKUP_TIME_SLOT_TO is null then to_timestamp(OPS.CUSTOMER_PICKUP_DATE)
# 	 when STORE_TYPE2 in ('HI') then concat(concat(to_date(OPS.CUSTOMER_PICKUP_DATE),' '),CUSTOMER_PICKUP_TIME_SLOT_TO) else to_timestamp(OPS.CREATED_AT) end as Customer_Pickup_Intent
# ,STORE_TYPE2,PM.PAYMENTHEADING,PM.Payment_Date
# ,case when OPS.STATUS = '0' then 'Pending'
# 	  when OPS.STATUS = '1' then 'Assigned'
# 	  when OPS.STATUS = '2' then 'Intransit'
# 	  when OPS.STATUS = '3' then 'Cancelled'
# 	  when OPS.STATUS = '-1' then 'Cancelled'
# 	  when OPS.STATUS = '4' then 'Completed' else null end AS STATUS_PICKUP
# ,TO_TIMESTAMP(CANCELLED_AT) AS CANCELLED_AT,TO_TIMESTAMP(OPS.IN_TRANSIT_AT) AS IN_TRANSIT_AT,TO_TIMESTAMP(OPS.ASSIGNED_AT) AS ASSIGNED_AT
# ,TO_TIMESTAMP(OPS.COMPLETE_ON) AS COMPLETE_ON, OPS.IS_RESCHEDULED, TO_TIMESTAMP(OPS.RESCHEDULED_AT) AS RESCHEDULED_AT
# ,Case when ORDERS.IS_SCRAP = 'NO' and OPS.VEHICLE_CONDITION = 'perfect' then 'Non Scrap & Perfect Working Condition Car' else 'Scrap & Non-Working Condition Car' end as Condition
# ,TO_TIMESTAMP(ORDERS.LAST_INSPECTION_DATE) AS INSPECTION_DATE,
# Case when OPS.DRIVER_TYPE = 1 then 'Inhouse'
# 	 when OPS.DRIVER_TYPE = 2 then 'Drive U'
# 	 when OPS.DRIVER_TYPE in (0,4,5,6,9) then 'Others'
#  	 when OPS.DRIVER_TYPE = 7 then 'Soos'
# 	 when OPS.DRIVER_TYPE = 8 then '21 North'
# 	 when OPS.DRIVER_TYPE = 10 then 'Breakdown - Towing'
# 	 when OPS.DRIVER_TYPE is null then 'Allocation missed but picked up' end as DRIVER_TYPE
# ,s.dealer_code,s.dealer_name,OPS.RESCHEDULING_REASON,OPS.TASK_ID,

# Case when orders.IS_DEAL_LOST_REQUEST = 0 then 'No'
# when orders.IS_DEAL_LOST_REQUEST = 1 then 'Yes'
# when orders.IS_DEAL_LOST_REQUEST = 2 then 'Pending At Finance' else 'NA' end as IS_DEAL_LOST
 
# FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS
# left join(select appointment_id,store_type as storetype,store_name from PC_STITCH_DB.DW.MARKETING_UNIVERSE) mu on mu.appointment_id=orders.lead_id
# LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_PICKUP_SERVICE OPS ON OPS.FK_ORDER_ID = ORDERS.ORDER_ID AND OPS.SERVICE_TYPE = 'PICKUP'

# Left JOIN (SELECT * FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY FK_ORDER_ID ORDER by ORDER_INVENTORY_LOG_ID ASC) AS rank 
# FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_INVENTORY_LOG 
# WHERE ACTION = 'CHECKED_IN')A WHERE rank = 1)OLA ON OLA.FK_ORDER_ID = ORDERS.ORDER_ID

# LEFT JOIN (Select *,Case when STORE_TYPE = 'PnS' then 'PnS'when LEFT(UPPER(CENTRE),3) = 'PLL' then 'PLL'
# 						 when STORE_TYPE = 'locality' then 'HI'
# 						 when STORE_TYPE = 'B2B' then 'B2B' else 'Retail' end as STORE_TYPE2 from PC_STITCH_DB.FIVETRAN1_BI.CENTRE_LIST) CL ON ORDERS.STORE_ID = CL.CENTRE_ID

# LEFT JOIN PC_STITCH_DB.FIVETRAN1_BI.PARKING P ON P.LOCATION_ID = OLA.FK_LOCATION_ID_TO
# LEFT JOIN (SELECT A.* FROM (SELECT LEAD_ID,PAYMENTHEADING,CREATED_AT  as Payment_Date,ROW_NUMBER() OVER (PARTITION BY LEAD_ID ORDER BY PAYMENTHEADING ASC) as rank
# FROM (SELECT LEAD_ID, CASE WHEN UPPER(PAYMENT_HEADING) IN ('DELIVERY','LOAN') THEN 'DELIVERY'
# 						   WHEN UPPER(PAYMENT_HEADING) IN ('TOKEN') THEN 'TOKEN' ELSE NULL END AS PAYMENTHEADING,TO_TIMESTAMP(OBI.CREATED_AT) AS CREATED_AT
# FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_BANK_INSTRUCTION OBI
# LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_PAYMENT_INSTRUCTION OPI ON OPI.ORDER_PAYMENT_INSTRUCTION_ID = OBI.FK_PAYMENT_INSTRUCTION
# LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_PURCHASE_REQUEST OPR ON OPR.ORDER_PURCHASE_REQUEST_ID = OPI.FK_PURCHASE_REQUEST
# LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS ON ORDERS.ORDER_ID = OPR.FK_ORDER_ID
# WHERE OPI.STATUS = 3 AND OBI.STATUS = 3 AND UPPER(PAYMENT_HEADING) IN ('DELIVERY','LOAN','TOKEN') ORDER BY PAYMENTHEADING ASC)A) A 
# WHERE rank = 1)PM ON to_char(PM.LEAD_ID) = to_char(ORDERS.LEAD_ID)
# LEFT JOIN PC_STITCH_DB.FIVETRAN1_BI.SALES_TRANSACTIONS S ON S.APPT_ID = ORDERS.LEAD_ID


# WHERE 
# --TO_DATE(OLA.CREATED_AT) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-95 AND 
# ORDERS.STATUS_ID <> 12 
# --AND ORDERS.IS_DEAL_LOST_REQUEST = 0 
# AND OPS.CREATED_AT IS NOT NULL --AND UPPER(CL.CENTRE) NOT LIKE ('%B2B%')
# --AND UPPER(Cl.centre) NOT LIKE '%-PNS%' AND UPPER(Cl.centre) NOT LIKE '%- PNS%'
# ORDER BY OLA.CREATED_AT ASC))T)T2
# Where TO_DATE(pickup_date_final) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-95)T2
# LEFT JOIN (Select APPOINTMENTID,INSPECTIONSTARTTIME_F from PC_STITCH_DB.WORK.FIRST_INSPECTION_DATA) FI ON T2.LEAD_ID = FI.APPOINTMENTID
# LEFT JOIN 
# (Select * from (Select *,
# ROW_NUMBER() OVER(PARTITION BY APP_ID ORDER BY LAST_OTP_SENT_DATE)as Rnk
#  from (SELECT split_part(REFERENCE_NO,'_',1) as App_ID,*
#  from PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.CUSTOMER_OTP)) WHERE Rnk = 1) OTP ON T2.LEAD_ID = OTP.APP_ID)T3)T4

# LEFT JOIN (Select ORDERS.LEAD_ID,ORDERS.ORDER_ID,OPR2.IS_PROJECT_LOST_LEAD,
# CASE WHEN IS_PROJECT_LOST_LEAD = 1 THEN 1 ELSE 0 END AS IS_PLL_CASE
# from PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS
# LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_PURCHASE_REQUEST OPR2 ON OPR2.FK_ORDER_ID = ORDERS.ORDER_ID AND OPR2.STATUS = 3)PLL
# ON T4.LEAD_ID = PLL.LEAD_ID)FT)


#         """
        
# cur.execute(query)
# rows = cur.fetchall()
# df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

# ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1CUWKMUGl5ninrWOzKGob1y3h3lxiC_EgGCw2N9qVsZw/edit#gid=0').worksheet('Raw Data')
# gd.set_with_dataframe(ws,df,resize=False,row=1,col=1)  #write
# # dff=pd.DataFrame(ws.get_all_records())  
# query1 = """WITH C2B_Inventory_log AS (Select * from (Select *,
# Case when ifnull(centre,RETAIL_CENTER) like '%PnS%' then 'PnS'when LEFT(UPPER(ifnull(centre,RETAIL_CENTER)),3) = 'PLL' then 'PLL'
# 						 when LEFT(UPPER(ifnull(centre,RETAIL_CENTER)),3) = 'HI-' then 'HI'
# 						 when ifnull(centre,RETAIL_CENTER) like '%B2B%' then 'B2B' else 'Retail' end as STORE_TYPE

# ,ROW_NUMBER() OVER(PARTITION BY LEAD_ID,ACTION_Created_at ORDER BY ACTION) as Rank
# --,ROW_NUMBER() OVER(PARTITION BY CONCAT(TO_CHAR(LEAD_ID)+ ' ' + ""ACTION"") ORDER BY ACTION_Created_at) as Rank2
# from
# (Select orders.lead_id,ol.PARKING as Latest_PARKING
# ,Case when UPPER(P.PARKING) like '%BANGALORE%' THEN 'BLR' ELSE P.REGION END as Parking_region
# ,r.org_name,orders.Registration_number,orders.make,orders.model,
# to_timestamp(ol.BUY_DATE) as BUY_DATE,to_timestamp(ol.SALE_DATE) as SALE_DATE,
# to_timestamp(ol.FIRST_SI) FIRST_SI,to_timestamp(ol.Latest_SI) Latest_SI,to_timestamp(ol.LATEST_SO) LATEST_SO,
# --ol.DEALER_CODE,
# oil.Action,il.location_name Location_To,il2.location_name Location_From,to_timestamp(oil.created_at) ACTION_Created_at,
# oil.CREATED_BY,ol.Ops_Status,
# Case when orders.IS_DEAL_LOST_REQUEST = 0 then 'No'
# when orders.IS_DEAL_LOST_REQUEST = 1 then 'Yes'
# when orders.IS_DEAL_LOST_REQUEST = 2 then 'Pending At Finance' else 'NA' end as IS_DEAL_LOST,
# CL.REGION AS RETAIL_REGION,CL.CENTRE Retail_Center,mu.store_name as centre,CL.CITY Retail_City
# --,LOCATION_ID parking_id
# --,rank()over(partition by orders.LEAD_ID order by ORDER_INVENTORY_LOG_ID)rank 
# from  PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_INVENTORY_LOG oil
# left join PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.INVENTORY_LOCATION il on il.inventory_location_id  = oil.fk_location_id_to
# left join PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.INVENTORY_LOCATION il2 on il2.INVENTORY_LOCATION_ID = oil.fk_location_id_from
# left join PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS on orders.order_id = oil.fk_order_id
# left join PC_STITCH_DB.FIVETRAN1_BI.RETAIL r on r.lead_id = orders.lead_id
# left join PC_STITCH_DB.FIVETRAN1_BI.PARKING p on p.parking = TRIM(il.location_name)
# left join PC_STITCH_DB.FIVETRAN1_BI.OPS_LOGISTICS ol on ol.lead_id = orders.lead_id
# LEFT JOIN PC_STITCH_DB.FIVETRAN1_BI.CENTRE_LIST CL ON ORDERS.STORE_ID = CL.CENTRE_ID
# left join(select appointment_id,store_type as storetype,store_name 
# from "PC_STITCH_DB"."DW"."MARKETING_UNIVERSE") mu on mu.appointment_id=orders.lead_id

# Where orders.IS_DEAL_LOST_REQUEST = 0
# ))Where Rank = 1 order by lead_id,ACTION_Created_at),

# CHECKED_IN_Data as
# (Select A.* from (Select LEAD_ID,REGISTRATION_NUMBER,MAKE,MODEL,FIRST_SI,RETAIL_REGION,centre,Retail_Center,Retail_City,STORE_TYPE,
# TRIM(LOCATION_TO) PARKING_NAME,Parking_Region,ACTION,ACTION_CREATED_AT as CHECKED_IN_AT,
# CREATED_BY AS CHECKED_IN_BY,OPS_STATUS,IS_DEAL_LOST
# ,rank()over(partition by LEAD_ID order by ACTION_CREATED_AT) CHECKED_IN_Cnt 
# from C2B_Inventory_log
# Where action = 'CHECKED_IN'
# AND to_date(FIRST_SI) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-95)a
# Where CHECKED_IN_Cnt = 1),

# Driver_QC_data as
# (Select * from (select appointment_id, verified_at as QC_approval_time, Submitted_at as QC_submission_time, created_at as driver_upload_start
# ,ROW_NUMBER() OVER(PARTITION BY appointment_id ORDER BY verified_at) as Rank
# from PC_STITCH_DB.DMS_DMS.DOCKET_VERIFICATION where submitted_by = 'RUNNER' 
# and verified_at IS NOT NULL and (date(created_At)>='2023-01-01')) Where rank = 1)

# (Select FSI.LEAD_ID,FSI.REGISTRATION_NUMBER,FSI.FIRST_SI,FSI.PARKING_NAME,QC.QC_APPROVAL_TIME,QC.QC_SUBMISSION_TIME,QC.DRIVER_UPLOAD_START

# from CHECKED_IN_Data FSI
# LEFT JOIN Driver_QC_data QC ON FSI.LEAD_ID = QC.appointment_id 

# LEFT JOIN "PC_STITCH_DB"."WEBSITE_DEALERENGINE_PROD"."APPOINTMENT" AP On  FSI.LEAD_ID=AP.PUB_APPT_ID

# order by FSI.FIRST_SI asc)

#         """     
# cur.execute(query1)
# rows = cur.fetchall()

# df2 = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1CUWKMUGl5ninrWOzKGob1y3h3lxiC_EgGCw2N9qVsZw/edit#gid=0').worksheet('Driver_QC')
gd.set_with_dataframe(ws,df2,resize=True,row=1,col=1)  #write
# dff=pd.DataFrame(ws.get_all_records())  
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1hUMmmrQIXDy2GR9TxfYjYdYLu9jpzTRSq52Hl24sitc/edit#gid=1892535976').worksheet('Sahil')
# gd.set_with_dataframe(ws1,df2,resize=True,row=1,col=1)  #write
fr=pd.DataFrame(ws.get_all_records())  
fr=fr[['Key','Reason for the Re-schedule:','Remarks (captured by driver):']]
ws=gc.open_by_url('https://docs.google.com/spreadsheets/d/1CUWKMUGl5ninrWOzKGob1y3h3lxiC_EgGCw2N9qVsZw/edit#gid=0').worksheet('G_form_response1')
gd.set_with_dataframe(ws,fr,resize=True,row=1,col=1)  #write
#fr=pd.DataFrame(ws.get_all_records())  
