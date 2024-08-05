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
import time as tm
import numpy as np
import warnings
warnings.filterwarnings("ignore")
import numpy as npx
import gspread
import gspread_dataframe as gd
email = 'sahil.5@cars24.com'
WAREHOUSE = 'BI_WH'

# Connect to Snowflake
ctx = snowflake.connector.connect(
    user=email,
    warehouse=WAREHOUSE,
    account='am62076.ap-southeast-2',
    authenticator="externalbrowser"  # Assuming you're using external browser authentication
)

cur = ctx.cursor()
gsheet_auth='C:/Users/35115/Desktop/Notebook/sahil_creds.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)

query = """

Select *,
case when FROM_LOCATIONTYPE='C2B_YARD' and TO_LOCATIONTYPE='TOUCH_POINT' then 'C2B_Yard to Dark Yard'
when FROM_LOCATIONTYPE='C2B_YARD' and  TO_LOCATIONTYPE='SERVICE_CENTER' and (TO_LOCATION  like '%IRC%' or  TO_LOCATION like '%MRL%') then 'C2B_Yard to MRL'
when FROM_LOCATIONTYPE='TOUCH_POINT' and TO_LOCATIONTYPE='SERVICE_CENTER' and TO_LOCATION like '%IRC%' then 'Spaze Palazo Parking to MRL Dharuhera'
when FROM_LOCATIONTYPE='TOUCH_POINT' and TO_LOCATIONTYPE='SERVICE_CENTER' and (TO_LOCATION not like '%IRC%' or  TO_LOCATION not like '%MRL%') then 'Dark Yard to Ext SC'
when FROM_LOCATIONTYPE='C2B_YARD'  and TO_LOCATIONTYPE='SERVICE_CENTER' and (TO_LOCATION not like '%IRC%' or  TO_LOCATION not like '%MRL%') then 'C2B_Yard to External SC'
WHEN (FROM_LOCATION NOT LIKE '%MRL _Studio%' OR FROM_LOCATION NOT LIKE '%IRC_Studio%') AND FROM_LOCATIONTYPE = 'SERVICE_CENTER' AND (TO_LOCATION NOT LIKE '%MRL _Studio%' OR TO_LOCATION NOT LIKE '%IRC_Studio%') AND TO_LOCATIONTYPE = 'CAR_STUDIO' THEN 'External SC to External Studio'
WHEN (FROM_LOCATION LIKE '%MRL _Studio%' OR FROM_LOCATION LIKE '%IRC_Studio%') AND FROM_LOCATIONTYPE = 'SERVICE_CENTER' AND (TO_LOCATION LIKE '%MRL _Studio%' OR TO_LOCATION LIKE '%IRC_Studio%') AND TO_LOCATIONTYPE = 'CAR_STUDIO' THEN 'SC to STUDIO'
when FROM_LOCATIONTYPE='TOUCH_POINT' and TO_LOCATIONTYPE='FULFILLMENT_CENTER' then 'Dark Yard to FC'
when FROM_LOCATIONTYPE='SERVICE_CENTER' and TO_LOCATIONTYPE='TOUCH_POINT' then 'MRL to Spaze Plazo'
when FROM_LOCATIONTYPE='CAR_STUDIO' and TO_LOCATIONTYPE='FULFILLMENT_CENTER' then 'STUDIO to FC'
when FROM_LOCATIONTYPE='FULFILLMENT_CENTER' and TO_LOCATIONTYPE='SERVICE_CENTER' and  (TO_LOCATION like '%IRC%' or  TO_LOCATION  like '%MRL%') then 'FC to MRL'
when FROM_LOCATIONTYPE='SERVICE_CENTER' and (FROM_LOCATION like '%IRC%' or FROM_LOCATION  like '%MRL%') and TO_LOCATIONTYPE='FULFILLMENT_CENTER' then 'MRL to FC'
when FROM_LOCATIONTYPE='SERVICE_CENTER' and (FROM_LOCATION not like '%IRC%' or  FROM_LOCATION  not like '%MRL%') and TO_LOCATIONTYPE='FULFILLMENT_CENTER' then 'External SC to FC'
when FROM_LOCATIONTYPE='FULFILLMENT_CENTER' and TO_LOCATIONTYPE='SERVICE_CENTER' and  (TO_LOCATION not like '%IRC%' or  TO_LOCATION  not like '%MRL%') then 'FC to Exteranl SC'
when FROM_LOCATIONTYPE='TOUCH_POINT' and   (TO_LOCATION  like '%IRC%' or  TO_LOCATION   like '%MRL%') then 'Dark Yard to MRL'
when FROM_LOCATIONTYPE='FULFILLMENT_CENTER' and TO_LOCATIONTYPE='TOUCH_POINT' then 'FC to Spaze Plazo'
when FROM_LOCATIONTYPE='SERVICE_CENTER' and (FROM_LOCATION like '%IRC%' or  FROM_LOCATION  like '%MRL%') and TO_LOCATIONTYPE='C2B_YARD' then 'MRL to C2B_ Yard (No Go Vehcile)'
when FROM_LOCATIONTYPE='FULFILLMENT_CENTER' and TO_LOCATIONTYPE ='CAR_STUDIO' then 'FC to Studio'
when FROM_LOCATIONTYPE='SERVICE_CENTER' and (FROM_LOCATION not like '%IRC%' or  FROM_LOCATION  not like '%MRL%') and TO_LOCATIONTYPE='C2B_YARD' then 'External SC to C2b_Yard ( No Go vehcile )'
when FROM_LOCATIONTYPE='CAR_STUDIO'  and TO_LOCATIONTYPE='SERVICE_CENTER' and  (TO_LOCATION not like '%IRC%' or  TO_LOCATION  not like '%MRL%') then 'Studio to external SC'
when FROM_LOCATIONTYPE='C2B_YARD'  and TO_LOCATIONTYPE='FULFILLMENT_CENTER'  then 'C2B Yard to FC'
when FROM_LOCATIONTYPE='FULFILLMENT_CENTER'  and TO_LOCATIONTYPE is null then 'FC to Cutomer Location ( Test Drive )'
when FROM_LOCATIONTYPE is null and TO_LOCATIONTYPE='FULFILLMENT_CENTER' then 'Test Drive to FC'
when FROM_LOCATIONTYPE='FULFILLMENT_CENTER'  and TO_LOCATIONTYPE='FULFILLMENT_CENTER' then 'FC to FC'
when FROM_LOCATIONTYPE is null and TO_LOCATIONTYPE='SERVICE_CENTER' then 'Customer to Refurb'
when FROM_LOCATIONTYPE='SERVICE_CENTER' and TO_LOCATIONTYPE is null then 'Refurb to customer'
when FROM_LOCATIONTYPE='SERVICE_CENTER' and TO_LOCATIONTYPE='SRVICE_CENTER' then 'SC to SC'
when FROM_LOCATIONTYPE='FULFILLMENT_CENTER' and TO_LOCATIONTYPE='C2B_YARD' then 'FC to C2B (Liquidation)'
when FROM_LOCATIONTYPE='CAR_STUDIO' and TO_LOCATIONTYPE='SERVICE_CENTER' and (TO_LOCATION not like '%IRC%' or  TO_LOCATION  not like '%MRL%') then 'Studio to external SC'
when FROM_LOCATIONTYPE='TOUCH_POINT' and TO_LOCATIONTYPE is null then 'Dark Yard to CUSTOMER'
when FROM_LOCATIONTYPE='CAR_STUDIO' and TO_LOCATIONTYPE='CAR_STUDIO' then 'STUDIO to STUDIO'
when FROM_LOCATIONTYPE is null and TO_LOCATIONTYPE='TOUCH_POINT' then 'Customer to Dark Yard' else 'SC to SC' end as Mov_types
,case when (FROM_LOCATION like '%VYR%' or FROM_LOCATION  like '%VYR%') then 'Virtual' else 'Actual' end as trip_type,concat(FROM_LOCATION,'-',TO_LOCATION) as Location_concat,
Case when (UPPER(FROM_LOCATION) like 'CARS24- YR%' or UPPER(TO_LOCATION) like 'CARS24- YR%' ) THEN 'Virtual_Trip'
WHEN (FROM_LOCATION like 'CARS24- VRY_%' or TO_LOCATION like 'CARS24- VRY_%') then 'VYR'
	 when MOVEMENT_TYPE = 'C2B_YARD to SC' then 'PTS'
	 when MOVEMENT_TYPE = 'SC to FC' then 'SC to FC'
	 	 when MOVEMENT_TYPE = 'FC to FC' then 'FC to FC'
		 when (MOVEMENT_TYPE ='SC to TOUCH_POINT' AND UPPER(TO_LOCATION) = 'SPAZE PLAZO')then 'SC to TOUCH_POINT'
		 when (MOVEMENT_TYPE ='TOUCH_POINT to SC' AND UPPER(FROM_LOCATION) = 'SPAZE PLAZO')then 'TOUCH_POINT to SC'
		 when (MOVEMENT_TYPE ='TOUCH_POINT to FC' AND UPPER(FROM_LOCATION) = 'SPAZE PLAZO')then 'TOUCH_POINT to FC'
		 
	 when FROM_LOCATIONTYPE = 'SERVICE_CENTER' then 'L1'
	 when TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'L2' else 'NA' END AS Movement_Type2

from (Select *,TO_CHAR(DT,'MON') AS month_name
from (With LOGISTIC_LOG as
(Select * from (Select *,
Case when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and REQUEST_TYPE = 'EXTERNAL' then 'FC to EXTERNAL'
     when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'FC to SC'
     when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'FC to FC'
     when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'FC to STUDIO'
     when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'C2B_YARD' then 'FC to C2B_YARD'
     when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'FC to TOUCH_POINT'
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'SC to TOUCH_POINT'	 
     when FROM_LOCATIONTYPE = 'TOUCH_POINT' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'TOUCH_POINT to FC'     

     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'STUDIO to TOUCH_POINT'     
     when FROM_LOCATIONTYPE = 'C2B_YARD' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'C2B_YARD to TOUCH_POINT'     
     when REQUEST_TYPE = 'RETURN' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'Return to TOUCH_POINT'
	 
     when FROM_LOCATIONTYPE = 'TOUCH_POINT' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'TOUCH_POINT to TOUCH_POINT'     
     when FROM_LOCATIONTYPE = 'TOUCH_POINT' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'TOUCH_POINT to SC'     
     when FROM_LOCATIONTYPE = 'TOUCH_POINT' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'TOUCH_POINT to STUDIO'     	 
	 
     when FROM_LOCATIONTYPE = 'TOUCH_POINT' and REQUEST_TYPE = 'EXTERNAL' then 'TOUCH_POINT to EXTERNAL'     	 
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'SC to FC'
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and TO_LOCATIONTYPE = 'C2B_YARD' then 'SC to C2B_YARD'
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'SC to SC'
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and REQUEST_TYPE = 'EXTERNAL' then 'SC to EXTERNAL'
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'STUDIO to FC'
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'STUDIO to SC'
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'STUDIO to STUDIO'
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and TO_LOCATIONTYPE = 'C2B_YARD' then 'STUDIO to C2B_YARD'
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and REQUEST_TYPE = 'EXTERNAL' then 'STUDIO to EXTERNAL'
     when REQUEST_TYPE = 'RETURN' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'Return to STUDIO'
     when REQUEST_TYPE = 'RETURN' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'Return to FC'
     when REQUEST_TYPE = 'RETURN' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'Return to SC'
     when FROM_LOCATIONTYPE = 'C2B_YARD' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'C2B_YARD to SC'
     when FROM_LOCATIONTYPE = 'C2B_YARD' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'C2B_YARD to FC'
     when FROM_LOCATIONTYPE = 'C2B_YARD' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'C2B_YARD to CAR_STUDIO'	 
     when FROM_LOCATIONTYPE = 'C2B_YARD' and REQUEST_TYPE = 'EXTERNAL' then 'C2B_YARD to EXTERNAL'     	 
     when FROM_LOCATIONTYPE = 'DEALER' and TO_LOCATIONTYPE = 'C2B_YARD' then 'DEALER to C2B_YARD'
     when FROM_LOCATIONTYPE = 'DEALER' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'DEALER to FC'
     when FROM_LOCATIONTYPE = 'DEALER' and REQUEST_TYPE = 'EXTERNAL' then 'DEALER to EXTERNAL'
     when FROM_LOCATIONTYPE = 'DEALER' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'DEALER to SC'
	 when (FROM_LOCATION = '_' and LOGISTIC_PROVIDER_TYPE = 'C2B' AND TO_LOCATIONTYPE = 'SERVICE_CENTER') then 'C2B_YARD to SC'
	 when (FROM_LOCATION = '_' and LOGISTIC_PROVIDER_TYPE = 'C2B' AND TO_LOCATIONTYPE = 'CAR_STUDIO') then 'C2B_YARD to CAR_STUDIO'
	 when (FROM_LOCATION = '_' and LOGISTIC_PROVIDER_TYPE = 'C2B' AND TO_LOCATIONTYPE = 'FULFILLMENT_CENTER') then 'C2B_YARD to FC'
	 ELSE concat(concat(FROM_LOCATION,'-'),TO_LOCATIONTYPE) END AS Movement_Type

 from (Select *,
Case when UPPER(Request_updated_by) like '%.COM%' then 'By_User'
when UPPER(Request_updated_by) like '%@CARS2%' then 'By_User'
when UPPER(Request_updated_by) like '%@CARP%' then 'By_User'
when UPPER(Request_updated_by) like '%.ORG%' then 'By_User'
when UPPER(Request_updated_by) like '%.IN%' then 'By_User'
else 'By_API' End as Updated_by
,concat(concat(concat(concat(concat(APPOINTMENTID,'-'),FROM_LOCATION),'-'),TO_LOCATION,'-'),INVENTORYMOVEMENTID) as concat

from (Select APPOINTMENTID,LOGISTIC.STATUS,LOGISTIC.LABEL,
parse_json(logistic):"driverId"::String as driverId,
parse_json(logistic):"driverName"::String as driverName,
parse_json(logistic):"driverMobileNo"::String as driverMobileNo,
parse_json(LOGISTIC):"status"::String as LOGISTIC_status,
parse_json(LOGISTIC):"clientBookingId"::String as LOGISTIC_clientBookingId,
parse_json(LOGISTIC):"providerType"::String as LOGISTIC_Provider_Type,
Case when parse_json(logistic):"requestedPickupTime"::String = '0021-MM-dd' then null else parse_json(logistic):"requestedPickupTime"::String end as requested_Pickup_Time,
parse_json(logistic):"pickUpSlotFrom"::String as pick_Up_Slot_From,
parse_json(logistic):"pickUpSlotTo"::String as pick_Up_Slot_To,
to_TIMESTAMP(cast(Dateadd(minute,330,LOGISTIC.UPDATEDAT) as timestamp))as REQUEST_UPDATEDAT,

IFNULL(IFNULL(LOCATION.NAME,parse_json(LOGISTIC.EXTERNALLOCATION):"location":"address"::String),'_') as FROM_LOCATION,
IFNULL(LOCATION.LOCATIONTYPE,LOGISTIC.FROMLOCATIONTYPE) as FROM_LOCATIONTYPE,
IFNULL(IFNULL(city.name,city2.name),parse_json(LOGISTIC.EXTERNALLOCATION):"location":"cityName"::String) as from_city,
IFNULL(IFNULL(LOCATION1.NAME,parse_json(LOGISTIC.EXTERNALLOCATION):"location":"address"::String),'_') as TO_LOCATION,
IFNULL(LOCATION1.LOCATIONTYPE,LOGISTIC.TOLOCATIONTYPE) as TO_LOCATIONTYPE,
IFNULL(IFNULL(city1.name,city3.name),parse_json(LOGISTIC.EXTERNALLOCATION):"location":"cityName"::String) as to_city,
LOGISTIC.INVENTORYMOVEMENTID,
parse_json(LOGISTIC.UPDATEDBY):"uid"::String as Request_updated_by,
case when parse_json(LOGISTIC.EXTERNALLOCATION):"addressType"::string = 'from' then 'RETURN' when parse_json(LOGISTIC.EXTERNALLOCATION):"location":"locationType"::String = 'EXTERNAL' then 'EXTERNAL' else 'INTERNAL' end as request_type
from ETL.ARANGO.INVENTORY_MOVEMENT_LOG_WH_VW LOGISTIC
LEFT JOIN ETL.ARANGO.INVENTORY_WH_VW INT on LOGISTIC.INVENTORYID = INT._KEY
LEFT JOIN ETL.ARANGO.LOCATION_VW LOCATION on LOCATION.CODE = LOGISTIC.FROMLOCATIONCODE
LEFT JOIN ETL.ARANGO.CITY_VW CITY ON CITY.CODE = LOCATION.citycode
LEFT JOIN ETL.ARANGO.LOCATION_VW LOCATION1 on LOCATION1.CODE = LOGISTIC.TOLOCATIONCODE
LEFT JOIN ETL.ARANGO.CITY_VW CITY1 ON CITY1.CODE = LOCATION1.citycode
LEFT JOIN ETL.ARANGO.CITY_VW CITY2 ON CITY2.CODE = LOGISTIC.FROMLOCATIONCITYCODE
LEFT JOIN ETL.ARANGO.CITY_VW CITY3 ON CITY3.CODE = LOGISTIC.TOLOCATIONCITYCODE
where LOGISTIC.VEHICLETYPE = 'CAR' and LOGISTIC.COUNTRYCODE = 'IN' 
and to_date(cast(Dateadd(minute,330,LOGISTIC.UPDATEDAT) as timestamp)) >= '2018-06-01')A) order by concat,REQUEST_UPDATEDAT)
Where TO_DATE(REQUEST_UPDATEDAT) >= '2021-06-01'
AND TO_DATE(REQUEST_UPDATEDAT) <= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))),



First_Stockin_SC_data AS
(Select APPOINTMENTID,LOCATION_NAME,LOCATIONTYPE,First_Stockin_AT_SC_datetime,First_Stockin_updated_by
 from (Select APPOINTMENTID,BOUGHT_AT,MODEL,REGISTRATION_NUMBER,FUEL_TYPE,YEAR,MAKE,OWNER_NUMBER,TRANSMISSION_TYPE
,INVENTORY,REQUEST_UPDATED_BY as First_Stockin_updated_by,INVENTORY_UPDATED_DATE First_Stockin_AT_SC_datetime,LOCATION_NAME,LOCATIONTYPE,CITY_NAME,UPDATED_BY
from
(Select INT_LOG_2.* from
(Select * from (Select LOCATION.NAME as LOCATION_NAME,LOCATION._KEY,LOCATION.LOCATIONTYPE,CITY.NAME as CITY_NAME,
ORDERS.MAKE,ORDERS.YEAR,ORDERS.MODEL,ORDERS.REGISTRATION_NUMBER,ORDERS.FUEL_TYPE,
ORDERS.OWNER_NUMBER,VARIANT.TRANSMISSION_TYPE,INT_LOG.*
,row_number() over(partition by APPOINTMENTID order by INVENTORY_UPDATED_DATE) as rank
from 
(Select *
from (Select *
from (Select Case when UPPER(Request_updated_by) like '%.COM%' then 'By_User'
when UPPER(Request_updated_by) like '%@CARS2%' then 'By_User'
when UPPER(Request_updated_by) like '%@CARP%' then 'By_User'
when UPPER(Request_updated_by) like '%.ORG%' then 'By_User'
when UPPER(Request_updated_by) like '%.IN%' then 'By_User'
else 'By_API' End as Updated_by, * from 
(Select APPOINTMENTID,to_timestamp(BOUGHTDATE) as BOUGHT_AT,LABEL as INVENTORY,
parse_json(UPDATEDBY):"uid"::String as Request_updated_by,INVENTORYID,
to_timestamp(dateadd(minutes,330,cast(UPDATEDAT as timestamp))) as INVENTORY_UPDATED_DATE,LOCATIONCODE
from ETL.ARANGO.INVENTORY_LOG_WH_VW where UPPER(VEHICLETYPE) = 'CAR' and LABEL = 'STOCK_IN' and COUNTRYCODE = 'IN'))
)) INT_LOG
LEFT JOIN ETL.ARANGO.LOCATION_VW LOCATION on LOCATION.CODE = INT_LOG.LOCATIONCODE
LEFT JOIN ETL.ARANGO.CITY_VW CITY ON CITY.CODE = LOCATION.CITYCODE
LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS ON ORDERS.LEAD_ID = INT_LOG.APPOINTMENTID
LEFT JOIN
(Select ODR.LEAD_ID,V.TRANSMISSION_TYPE from PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS ODR
Left JOIN "PC_STITCH_DB"."WEBSITE_DEALERENGINE_PROD"."VARIANT" v on v.VARIANT_ID=ODR.VARIANTID) VARIANT ON ORDERS.LEAD_ID = VARIANT.LEAD_ID
Where LOCATION.LOCATIONTYPE = 'SERVICE_CENTER') Where rank = 1)INT_LOG_2)T
Order by LOCATION_NAME,INVENTORY_UPDATED_DATE desc)T2
Where to_date(First_Stockin_AT_SC_datetime) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-31),


C2B_Inventory_Logs AS
(Select * from (select orders.lead_id,ol.PARKING as Latest_PARKING
,r.org_name,orders.Registration_number,orders.make,orders.model,
to_timestamp(ol.BUY_DATE) as BUY_DATE,to_timestamp(ol.SALE_DATE) as SALE_DATE,
to_timestamp(ol.FIRST_SI) FIRST_SI,to_timestamp(ol.Latest_SI) Latest_SI,to_timestamp(ol.LATEST_SO) LATEST_SO,
oil.Action,il.location_name Location_To,il2.location_name Location_From,to_timestamp(oil.created_at) ACTION_Created_at,
oil.CREATED_BY,ol.Ops_Status,
Case when orders.IS_DEAL_LOST_REQUEST = 0 then 'No'
when orders.IS_DEAL_LOST_REQUEST = 1 then 'Yes'
when orders.IS_DEAL_LOST_REQUEST = 2 then 'Pending At Finance' else 'NA' end as IS_DEAL_LOST
from  PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_INVENTORY_LOG oil
left join PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.INVENTORY_LOCATION il on il.inventory_location_id  = oil.fk_location_id_to
left join PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.INVENTORY_LOCATION il2 on il2.INVENTORY_LOCATION_ID = oil.fk_location_id_from
left join PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS on orders.order_id = oil.fk_order_id
left join PC_STITCH_DB.FIVETRAN1_BI.RETAIL r on r.lead_id = orders.lead_id
left join PC_STITCH_DB.FIVETRAN1_BI.PARKING p on p.parking = il.location_name
left join PC_STITCH_DB.FIVETRAN1_BI.OPS_LOGISTICS ol on ol.lead_id = orders.lead_id)a
Where ACTION = 'PTS' 
order by lead_id,ACTION_Created_at),

C2B_PTS_Data as 
(Select LEAD_ID,LOCATION_FROM,PTS_Completion,CREATED_BY,OPS_STATUS from
(Select LEAD_ID,LOCATION_FROM,ACTION_CREATED_AT PTS_Completion,CREATED_BY,OPS_STATUS,
ACTION,rank()over(partition by LEAD_ID order by ACTION_CREATED_AT desc)rank 
from C2B_Inventory_Logs) T
where rank = 1),


INVENTORY_LOG as
(Select APPOINTMENTID,IV.CITYCODE,
to_timestamp(cast(Dateadd(minute,330,IV.CREATEDAT) as timestamp)) as Bought_dt,
LOCATION.name as location_name
,IV.LABEL,IV.LOCATIONTYPE,
to_timestamp(cast(Dateadd(minute,330,IV.UPDATEDAT) as timestamp)) as REQUEST_UPDATEDAT,CITY.Name as CITY
from ETL.ARANGO.INVENTORY_LOG_WH_VW IV
LEFT JOIN ETL.ARANGO.LOCATION_VW LOCATION on LOCATION.CODE = IV.LOCATIONCODE
LEFT JOIN ETL.ARANGO.CITY_VW CITY ON CITY.CODE = LOCATION.citycode
where IV.COUNTRYCODE = 'IN' and IV.VEHICLETYPE = 'CAR' order by IV._KEY DESC),

GS_INVENTORY_Stockin as 
(Select APPOINTMENTID,LOCATION_NAME,
MIN(REQUEST_UPDATEDAT) AS First_Stockin
,MAX(REQUEST_UPDATEDAT) AS Latest_Stockin from INVENTORY_LOG
Where LABEL = 'STOCK_IN' AND LOCATIONTYPE = 'FULFILLMENT_CENTER'
Group by 1,2),


LOGISTIC_Data as
(Select T.*,ORDERS.REGISTRATION_NUMBER,PTS.PTS_COMPLETION,PTS.CREATED_BY AS PTS_CREATED_BY
 from (Select *,
to_date(REQUEST_UPDATED_AT) DT
 from 
(Select *,
Case when FROM_CITY2 = TO_CITY2 then 'Same City' else 'Other City' end as City_Flag

,
Case when DONE_AT is not null then DONE_AT
when CLOUSER_STATUS = 'CANCELLED' then CANCELLED_AT
when CLOUSER_STATUS = 'IN_TRANSIT' then IN_TRANSIT_AT
when CLOUSER_STATUS = 'DENIED' then DENIED_AT
when CLOUSER_STATUS = 'DISPUTED' then DISPUTED_AT
when CLOUSER_STATUS = 'ACCEPTED' then ACCEPTED_AT
when CLOUSER_STATUS = 'DRIVER_ON_WAY' then DRIVER_ON_WAY_AT
when CLOUSER_STATUS = 'DRIVER_ASSIGNED' then DRIVER_ASSIGNED_AT
when CLOUSER_STATUS = 'REQUESTED' then REQUEST_CREATED_AT
when CLOUSER_STATUS = 'PENDING' then REQUEST_CREATED_AT
else REQUEST_CREATED_AT end as REQUEST_UPDATED_AT

,Case when DONE_AT is not null then 'Done'
when CLOUSER_STATUS = 'CANCELLED' then 'CANCELLED'
when CLOUSER_STATUS = 'IN_TRANSIT' then 'IN_TRANSIT'
when CLOUSER_STATUS = 'DENIED' then 'DENIED'
when CLOUSER_STATUS = 'DISPUTED' then 'DISPUTED'
when CLOUSER_STATUS = 'ACCEPTED' then 'ACCEPTED'
when CLOUSER_STATUS = 'DRIVER_ON_WAY' then 'DRIVER_ON_WAY'
when CLOUSER_STATUS = 'DRIVER_ASSIGNED' then 'DRIVER_ASSIGNED'
when CLOUSER_STATUS = 'REQUESTED' then 'REQUESTED'
when CLOUSER_STATUS = 'PENDING' then 'PENDING' else CLOUSER_STATUS end as Final_Status

 from

(Select 
APPOINTMENTID,REQUESTED_PICKUP_TIME,PICK_UP_SLOT_FROM,PICK_UP_SLOT_TO,REQUEST_UPDATEDAT REQUEST_CREATED_AT

,Request_updated_by as Request_Created_by,Updated_by as Created_by


,FROM_LOCATION,FROM_LOCATIONTYPE,FROM_CITY,TO_LOCATION,TO_LOCATIONTYPE,TO_CITY,REQUEST_TYPE
,concat
,LOGISTIC_Provider__Type,LOGISTIC__clientBookingId
,Driver_ID,Driver_Name,Driver_MobileNo
,Request_closed_by,Closed_by
,DONE_AT,IN_TRANSIT_AT,IN_TRANSIT_DONE_AT,DRIVER_ON_WAY_AT,ACCEPTED_AT,DRIVER_ASSIGNED_AT,DENIED_AT,DISPUTED_AT,CANCELLED_AT,CLOUSER_STATUS,
CASE WHEN UPPER(FROM_CITY) = 'BANGALORE CENTRAL' THEN 'BENGALURU'
 WHEN UPPER(FROM_CITY) = 'ZIRAKPUR' THEN 'CHANDIGARH'
 WHEN UPPER(FROM_CITY) = 'GURGAON' THEN 'DELHI/NCR'
 WHEN UPPER(FROM_CITY) = 'DELHI-NCR' THEN 'DELHI/NCR'
 WHEN UPPER(FROM_CITY) = 'NEW DELHI' THEN 'DELHI/NCR'
 WHEN UPPER(FROM_CITY) = 'DELHI' THEN 'DELHI/NCR'
 WHEN UPPER(FROM_CITY) = 'NOIDA' THEN 'DELHI/NCR'
 WHEN UPPER(FROM_CITY) = 'GREATER NOIDA' THEN 'DELHI/NCR' 
 WHEN UPPER(FROM_CITY) = 'FARIDABAD' THEN 'DELHI/NCR'
 WHEN UPPER(FROM_CITY) = 'GHAZIABAD' THEN 'DELHI/NCR'
 WHEN UPPER(FROM_CITY) = 'VISHAKAPATNAM' THEN 'VISAKHAPATNAM' ELSE UPPER(FROM_CITY) END AS FROM_CITY2,
 
CASE WHEN UPPER(TO_CITY) = 'BANGALORE CENTRAL' THEN 'BENGALURU'
 WHEN UPPER(TO_CITY) = 'ZIRAKPUR' THEN 'CHANDIGARH'
 WHEN UPPER(TO_CITY) = 'GURGAON' THEN 'DELHI/NCR'
 WHEN UPPER(TO_CITY) = 'DELHI-NCR' THEN 'DELHI/NCR'
 WHEN UPPER(TO_CITY) = 'NEW DELHI' THEN 'DELHI/NCR'
 WHEN UPPER(TO_CITY) = 'DELHI' THEN 'DELHI/NCR'
 WHEN UPPER(TO_CITY) = 'NOIDA' THEN 'DELHI/NCR'
 WHEN UPPER(TO_CITY) = 'GREATER NOIDA' THEN 'DELHI/NCR'  
 WHEN UPPER(TO_CITY) = 'FARIDABAD' THEN 'DELHI/NCR'
 WHEN UPPER(TO_CITY) = 'GHAZIABAD' THEN 'DELHI/NCR'
 WHEN UPPER(TO_CITY) = 'VISHAKAPATNAM' THEN 'VISAKHAPATNAM' ELSE UPPER(TO_CITY) END AS TO_CITY2,

Case when (UPPER(FROM_LOCATION) like 'CARS24- YR%' or UPPER(TO_LOCATION) like 'CARS24- YR%' ) THEN 'Virtual_Trip'
	  when (FROM_LOCATIONTYPE = 'C2B_YARD' and TO_LOCATIONTYPE = 'SERVICE_CENTER') then 'First_Mile'
	  when (FROM_LOCATION = '_' and LOGISTIC_PROVIDER__TYPE = 'C2B' AND TO_LOCATIONTYPE = 'SERVICE_CENTER') then 'First_Mile'
	  when (REQUEST_TYPE = 'INTERNAL' and MOVEMENT_TYPE <> 'FC to FC') then 'Middle_Mile'
	  when REQUEST_TYPE in ('EXTERNAL','RETURN') or (REQUEST_TYPE = 'INTERNAL' and FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER')
	  then 'Last_Mile' else 'Others' end as Leg_Type


 from

(Select Total_Requests.*,Last_Status.LOGISTIC_Provider_Type as LOGISTIC_Provider__Type
,Last_Status.LOGISTIC_clientBookingId as LOGISTIC__clientBookingId
,Status_Update_at.DONE as DONE_At
,Status_Update_at.IN_TRANSIT as IN_TRANSIT_At
,Status_Update_at.IN_TRANSIT_DONE as IN_TRANSIT_DONE_At
,Status_Update_at.CANCELLED as CANCELLED_At
,Status_Update_at.DRIVER_ON_WAY as DRIVER_ON_WAY_At
,Status_Update_at.ACCEPTED as ACCEPTED_At
,Status_Update_at.DISPUTED as DISPUTED_At
,Status_Update_at.DRIVER_ASSIGNED as DRIVER_ASSIGNED_At
,Status_Update_at.DENIED as DENIED_At
,Last_Status.LABEL AS Clouser_Status
,Last_Status.driverId AS Driver_ID
,Last_Status.driverName AS Driver_Name
,Last_Status.driverMobileNo AS Driver_MobileNo
,Last_Status.Request_updated_by as Request_closed_by
,Last_Status.Updated_by as Closed_by

from 
(Select * from (Select *
,ROW_NUMBER() OVER(PARTITION BY LL.concat ORDER BY LL.REQUEST_UPDATEDAT ASC)as Rank
from LOGISTIC_LOG LL
where LL.LABEL in ('REQUESTED','PENDING')) Where Rank = 1) Total_Requests
LEFT JOIN 
(Select APPOINTMENTID,concat
,MIN(Case WHEN LABEL = 'DONE' then REQUEST_UPDATEDAT end) as Done
,MIN(Case WHEN LABEL = 'IN_TRANSIT' then REQUEST_UPDATEDAT end) as IN_TRANSIT
,MIN(Case WHEN LABEL = 'IN_TRANSIT' and LOGISTIC_STATUS = 'DONE'then REQUEST_UPDATEDAT end) as IN_TRANSIT_DONE
,MIN(Case WHEN LABEL = 'CANCELLED' then REQUEST_UPDATEDAT end) as CANCELLED
,MIN(Case WHEN LABEL = 'DRIVER_ON_WAY' then REQUEST_UPDATEDAT end) as DRIVER_ON_WAY
,MIN(Case WHEN LABEL = 'ACCEPTED' then REQUEST_UPDATEDAT end) as  ACCEPTED
,MIN(Case WHEN LABEL = 'DISPUTED' then REQUEST_UPDATEDAT end) as DISPUTED
,MIN(Case WHEN LABEL = 'DRIVER_ASSIGNED' then REQUEST_UPDATEDAT end) as  DRIVER_ASSIGNED
,MIN(Case WHEN LABEL = 'DENIED' then REQUEST_UPDATEDAT end) as DENIED

from LOGISTIC_LOG
Where LABEL in ('DONE','IN_TRANSIT','CANCELLED','DRIVER_ON_WAY','ACCEPTED','DISPUTED','DRIVER_ASSIGNED','DENIED')
group by 1,2)Status_Update_at
 
ON Total_Requests.APPOINTMENTID = Status_Update_at.APPOINTMENTID AND Total_Requests.CONCAT = Status_Update_at.CONCAT

LEFT JOIN

(Select * from (Select APPOINTMENTID,CONCAT,REQUEST_UPDATEDAT,LABEL,LOGISTIC_Provider_Type,driverId,driverName,driverMobileNo
,LOGISTIC_clientBookingId,Request_updated_by,Updated_by
,ROW_NUMBER() OVER(PARTITION BY concat ORDER BY REQUEST_UPDATEDAT DESC)as Rank
from LOGISTIC_LOG)T Where Rank = 1) Last_Status ON
Total_Requests.APPOINTMENTID = Last_Status.APPOINTMENTID AND Total_Requests.CONCAT = Last_Status.CONCAT))))T
Left Join PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS ON T.APPOINTMENTID = ORDERS.LEAD_ID
LEFT JOIN C2B_PTS_Data PTS ON T.APPOINTMENTID = PTS.LEAD_ID),

-----------
LOGISTIC_Data_Final as
(Select *,
Case when Final_AGEING_Logistic <=0 then '0' 
	 when Final_AGEING_Logistic <=3 then to_char(Final_AGEING_Logistic)
	 when Final_AGEING_Logistic>3 then '3+' end as Final_AGEING_Slab_Logistic
,Case when Final_AGEING_PTS <=0 then '0' 
	 when Final_AGEING_PTS <=3 then to_char(Final_AGEING_PTS)
	 when Final_AGEING_PTS>3 then '3+' else '' end as Final_AGEING_Slab_PTS

 from
(Select *,
Case when FINAL_STATUS = 'Done' 
then DATEDIFF(DAYS,GREATEST(TO_DATE(REQUESTED_PICKUP_TIME),TO_DATE(FINAL_REQUEST_CREATED)),to_date(REQUEST_UPDATED_AT))
else DATEDIFF(DAYS,GREATEST(TO_DATE(REQUESTED_PICKUP_TIME),TO_DATE(FINAL_REQUEST_CREATED)),
to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))) end as Final_AGEING_Logistic

,Case when (LEG_TYPE = 'First_Mile' AND  FINAL_STATUS = 'Done' and PTS_COMPLETION is not null)
THEN DATEDIFF(DAYS,GREATEST(TO_DATE(REQUESTED_PICKUP_TIME),TO_DATE(FINAL_REQUEST_CREATED)),to_date(PTS_COMPLETION))

when (LEG_TYPE = 'First_Mile' AND  FINAL_STATUS = 'Done' and PTS_COMPLETION is null)
THEN DATEDIFF(DAYS,GREATEST(TO_DATE(REQUESTED_PICKUP_TIME),TO_DATE(FINAL_REQUEST_CREATED)),to_date(REQUEST_UPDATED_AT))

WHEN (LEG_TYPE = 'First_Mile' AND  FINAL_STATUS <> 'Done') 
THEN DATEDIFF(DAYS,GREATEST(TO_DATE(REQUESTED_PICKUP_TIME),TO_DATE(FINAL_REQUEST_CREATED)),
to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))) END as Final_AGEING_PTS

 from 
(Select *,dateadd(DAY, After_4_PM_on_Week_Days+Sat_Sun+Holiday,REQUEST_CREATED_AT) as Final_REQUEST_CREATED from
(Select *,

Case when (CREATED_DAY = 'Sat' AND REQUEST_AFTER_4PM = 1
AND DATEDIFF(DAYS,TO_DATE(REQUEST_CREATED_AT),to_date(REQUEST_UPDATED_AT))>1) THEN 2
when (CREATED_DAY = 'Sat' AND REQUEST_AFTER_4PM = 1
AND DATEDIFF(DAYS,TO_DATE(REQUEST_CREATED_AT),to_date(REQUEST_UPDATED_AT))>0) THEN 1
when (CREATED_DAY = 'Sat' AND REQUEST_AFTER_4PM = 0 
AND DATEDIFF(DAYS,TO_DATE(REQUEST_CREATED_AT),to_date(REQUEST_UPDATED_AT))>0) THEN 1
when (CREATED_DAY = 'Sun' AND DATEDIFF(DAYS,TO_DATE(REQUEST_CREATED_AT),to_date(REQUEST_UPDATED_AT))>0) THEN 1
else 0 end as Sat_Sun,

Case when CREATED_DAY in ('Sat','Sun') then 0
	 when CREATED_DAY not in ('Sat','Sun') and REQUEST_AFTER_4PM = 1 then 1 else 0 end as After_4_PM_on_Week_Days,

Case when to_date(REQUEST_CREATED_AT) in ('2024-01-01','2024-01-26','2024-03-25','2024-04-10','2024-08-15','2024-08-19','2024-10-02','2024-10-12','2024-10-31','2024-11-01','2024-11-15')
     then 1 else 0 end as Holiday,

 Case when to_date(REQUEST_CREATED_AT) = '2024-01-01' then 'New Year'
     when to_date(REQUEST_CREATED_AT) = '2024-01-26' then 'Republic Day'
     when to_date(REQUEST_CREATED_AT) = '2024-03-25' then 'Holi'
     when to_date(REQUEST_CREATED_AT) = '2024-04-10' then 'Eid-ul fitar'
     when to_date(REQUEST_CREATED_AT) = '2024-08-15' then 'Independence Day'
     when to_date(REQUEST_CREATED_AT) = '2024-08-19' then 'Raksha Bandhan'
     when to_date(REQUEST_CREATED_AT) = '2024-10-02' then 'Gandhi Jayanti'
     when to_date(REQUEST_CREATED_AT) = '2024-10-12' then 'Dussehra'
     when to_date(REQUEST_CREATED_AT) = '2024-10-31' then 'Deepawali'
	 when to_date(REQUEST_CREATED_AT) = '2024-11-01' then 'Deepawali/ Gujarati New Year/ Gowardhan Pooja'
     when to_date(REQUEST_CREATED_AT) = '2024-11-15' then 'Guru Nanak Jayanti'
	 
     else '-'
     end as Occasion_Festival
from
(Select *
,Case when month(dt) = month(to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-1) then 1 else 0 end as Current_Mnt
,CASE WHEN MONTH(to_date(dt)) = MONTH(((current_date() - interval '1 day')-interval '1 month'))
 AND YEAR(to_date(dt)) = YEAR(((current_date() - interval '1 day')-interval '1 month'))  then 1 else 0 end as Last_Mnt 
 ,Case when dt between (to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-7) and (to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-1) then 1 else 0 end as Last7_Days
,Case when dt = (to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-1) then 1 else 0 end as Last_Day
,dayname(to_date(REQUEST_CREATED_AT)) as Created_Day,cast(REQUEST_CREATED_AT as time) REQUEST_CREATED_Time,
Case when hour(REQUEST_CREATED_AT) >= 16 then 1 else 0 end as Request_After_4PM
 from LOGISTIC_Data))))),

Final_LOGISTIC_Data_v2 as

(Select a.*,b.REGIONCODE as REGION_CODE
,
Case when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and REQUEST_TYPE = 'EXTERNAL' then 'FC to EXTERNAL'
     when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'FC to FC'
     when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'FC to SC'
     when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'FC to STUDIO'
     when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'C2B_YARD' then 'FC to C2B_YARD'
     when FROM_LOCATIONTYPE = 'FULFILLMENT_CENTER' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'FC to TOUCH_POINT'
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'SC to TOUCH_POINT'	 
     when FROM_LOCATIONTYPE = 'TOUCH_POINT' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'TOUCH_POINT to FC'     
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'STUDIO to TOUCH_POINT'     
     when FROM_LOCATIONTYPE = 'C2B_YARD' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'C2B_YARD to TOUCH_POINT'     
     when REQUEST_TYPE = 'RETURN' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'Return to TOUCH_POINT'
	 when FROM_LOCATIONTYPE = 'TOUCH_POINT' and TO_LOCATIONTYPE = 'TOUCH_POINT' then 'TOUCH_POINT to TOUCH_POINT'     
     when FROM_LOCATIONTYPE = 'TOUCH_POINT' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'TOUCH_POINT to SC'     
     when FROM_LOCATIONTYPE = 'TOUCH_POINT' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'TOUCH_POINT to STUDIO'     	 
	 
     when FROM_LOCATIONTYPE = 'TOUCH_POINT' and REQUEST_TYPE = 'EXTERNAL' then 'TOUCH_POINT to EXTERNAL'     	 
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'SC to STUDIO'
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'SC to FC'
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and TO_LOCATIONTYPE = 'C2B_YARD' then 'SC to C2B_YARD'
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'SC to SC'
     when FROM_LOCATIONTYPE = 'SERVICE_CENTER' and REQUEST_TYPE = 'EXTERNAL' then 'SC to EXTERNAL'
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'STUDIO to FC'
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'STUDIO to SC'
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'STUDIO to STUDIO'
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and TO_LOCATIONTYPE = 'C2B_YARD' then 'STUDIO to C2B_YARD'
     when FROM_LOCATIONTYPE = 'CAR_STUDIO' and REQUEST_TYPE = 'EXTERNAL' then 'STUDIO to EXTERNAL'
     when REQUEST_TYPE = 'RETURN' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'Return to STUDIO'
     when REQUEST_TYPE = 'RETURN' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'Return to FC'
     when REQUEST_TYPE = 'RETURN' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'Return to SC'
     when FROM_LOCATIONTYPE = 'C2B_YARD' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'C2B_YARD to SC'
     when FROM_LOCATIONTYPE = 'C2B_YARD' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'C2B_YARD to FC'
     when FROM_LOCATIONTYPE = 'C2B_YARD' and TO_LOCATIONTYPE = 'CAR_STUDIO' then 'C2B_YARD to CAR_STUDIO'	 
     when FROM_LOCATIONTYPE = 'C2B_YARD' and REQUEST_TYPE = 'EXTERNAL' then 'C2B_YARD to EXTERNAL'     	 
     when FROM_LOCATIONTYPE = 'DEALER' and TO_LOCATIONTYPE = 'C2B_YARD' then 'DEALER to C2B_YARD'
     when FROM_LOCATIONTYPE = 'DEALER' and TO_LOCATIONTYPE = 'FULFILLMENT_CENTER' then 'DEALER to FC'
     when FROM_LOCATIONTYPE = 'DEALER' and REQUEST_TYPE = 'EXTERNAL' then 'DEALER to EXTERNAL'
     when FROM_LOCATIONTYPE = 'DEALER' and TO_LOCATIONTYPE = 'SERVICE_CENTER' then 'DEALER to SC'
	 when (FROM_LOCATION = '_' and LOGISTIC_Provider__Type = 'C2B' AND TO_LOCATIONTYPE = 'SERVICE_CENTER') then 'C2B_YARD to SC'
	 when (FROM_LOCATION = '_' and LOGISTIC_Provider__Type = 'C2B' AND TO_LOCATIONTYPE = 'CAR_STUDIO') then 'C2B_YARD to CAR_STUDIO'
	 when (FROM_LOCATION = '_' and LOGISTIC_Provider__Type = 'C2B' AND TO_LOCATIONTYPE = 'FULFILLMENT_CENTER') then 'C2B_YARD to FC'
	 ELSE concat(concat(FROM_LOCATION,'-'),TO_LOCATIONTYPE) END AS Movement_Type

from LOGISTIC_Data_Final a

Left Join
(Select * from (Select loc.NAME,loc.CODE,city.name as city,
loc.STATECODE,
Case when loc.STATECODE = 'DL' then 'NDL'
     when loc.STATECODE = 'PB' then 'NPB'
     when loc.STATECODE = 'UP' then 'NUP'
     when loc.STATECODE = 'AP' then 'SAP'
     when loc.STATECODE = 'TN' then 'STN'
     when loc.STATECODE = 'MH' then 'WMH'
     when loc.STATECODE = 'UK' then 'NUK'
     when loc.STATECODE = 'OD' then 'EOR'     
     when CITY = 'Panvel' then 'WMH'
     when CITY = 'Thane' then 'WMH'     
     when CITY = 'Kolkata' then 'EWB'
     when CITY = 'Chandigarh' then 'NPB'
else loc.REGIONCODE end as REGIONCODE
,ROW_NUMBER() OVER(PARTITION BY loc.NAME ORDER BY loc.CREATEDAT ASC)as Rank
from ETL.ARANGO.LOCATION_VW loc
LEFT JOIN ETL.ARANGO.CITY_VW city on loc.citycode=city.code
WHERE loc.COUNTRYCODE = 'IN') Where rank =1)b ON a.FROM_LOCATION = b.Name
Where CLOUSER_STATUS is not null)

Select APPOINTMENTID,REQUESTED_PICKUP_TIME,PICK_UP_SLOT_FROM,PICK_UP_SLOT_TO,
REPLACE(REQUEST_CREATED_AT,'.000','') AS REQUEST_CREATED_AT,REQUEST_CREATED_BY,CREATED_BY,FROM_LOCATION,FROM_LOCATIONTYPE,
FROM_CITY,TO_LOCATION,TO_LOCATIONTYPE,TO_CITY,REQUEST_TYPE,CONCAT,LOGISTIC_PROVIDER__TYPE,LOGISTIC__CLIENTBOOKINGID,DRIVER_ID,DRIVER_NAME,DRIVER_MOBILENO,
REQUEST_CLOSED_BY,CLOSED_BY,
REPLACE(DONE_AT,'.000','') AS DONE_AT,
REPLACE(IN_TRANSIT_AT,'.000','') AS IN_TRANSIT_AT,
REPLACE(DRIVER_ON_WAY_AT,'.000','') AS DRIVER_ON_WAY_AT,
REPLACE(ACCEPTED_AT,'.000','') AS ACCEPTED_AT,
REPLACE(DRIVER_ASSIGNED_AT,'.000','') AS DRIVER_ASSIGNED_AT,
REPLACE(DENIED_AT,'.000','') AS DENIED_AT,
REPLACE(DISPUTED_AT,'.000','') AS DISPUTED_AT,
REPLACE(CANCELLED_AT,'.000','') AS CANCELLED_AT,
CLOUSER_STATUS,FROM_CITY2,TO_CITY2,LEG_TYPE,CITY_FLAG,REPLACE(REQUEST_UPDATED_AT,'.000','') AS REQUEST_UPDATED_AT,
FINAL_STATUS,DT,REGISTRATION_NUMBER,REPLACE(PTS_COMPLETION,'.000','') AS PTS_COMPLETION,PTS_CREATED_BY,Current_Mnt,Last_Mnt,
Last7_Days,Last_Day,CREATED_DAY,REQUEST_CREATED_TIME,REQUEST_AFTER_4PM,SAT_SUN,AFTER_4_PM_ON_WEEK_DAYS,HOLIDAY,OCCASION_FESTIVAL,REPLACE(FINAL_REQUEST_CREATED,'.000','') AS FINAL_REQUEST_CREATED,
FINAL_AGEING_LOGISTIC,FINAL_AGEING_PTS,FINAL_AGEING_SLAB_LOGISTIC,FINAL_AGEING_SLAB_PTS,REGION_CODE,MOVEMENT_TYPE,IS_TRIP_CLOSED,CANCELLED_DENIED,L3D,
TRIP_STATUS,REPLACE(FIRST_STOCKIN_AT_SC_DATETIME,'.000','') AS FIRST_STOCKIN_AT_SC_DATETIME,REPLACE(IN_TRANSIT_DONE_AT,'.000','') AS IN_TRANSIT_DONE_AT
,MAKE,MODEL,FUEL_TYPE,C24_QUOTE

 from 
(Select T.*,SC.FIRST_STOCKIN_AT_SC_DATETIME from
(Select *
 ,Case when FINAL_STATUS = 'Done' then 1 else 0 end as IS_TRIP_Closed
 ,Case when UPPER(CLOUSER_STATUS) in ('CANCELLED','DENIED') then 1 else 0 end as Cancelled_Denied
 ,Case when DT >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-3 
 AND  DT <= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-1 then 1 else 0 end as L3D
,Case when FINAL_STATUS = 'Done' THEN 'Done'
      when FINAL_STATUS = 'DENIED' then 'Denied'
      when FINAL_STATUS = 'CANCELLED' then 'Cancelled'
      when FINAL_STATUS = 'DISPUTED' then 'Disputed' 
      when FINAL_STATUS in ('PENDING','REQUESTED') then 'Pending'
      when FINAL_STATUS in ('IN_TRANSIT','ACCEPTED','DRIVER_ON_WAY','DRIVER_ASSIGNED') then 'Open'
 end as Trip_Status
 
from Final_LOGISTIC_Data_v2)T
LEFT JOIN First_Stockin_SC_data SC ON T.APPOINTMENTID = SC.APPOINTMENTID AND T.TO_LOCATION = SC.LOCATION_NAME
Where (IS_TRIP_Closed = 1  and FROM_LOCATION <> '_' 
and TO_DATE(REQUEST_UPDATED_AT) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-100
AND TO_DATE(REQUEST_UPDATED_AT) <= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))
or IS_TRIP_Closed = 0 and CLOUSER_STATUS in ('CANCELLED','DENIED')
and TO_DATE(REQUEST_UPDATED_AT) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-100
AND TO_DATE(REQUEST_UPDATED_AT) <= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))
       and FROM_LOCATION <> '_'
or IS_TRIP_Closed = 0 and CLOUSER_STATUS not in ('CANCELLED','DENIED')
and FROM_LOCATION <> '_'
      ))TF
LEFT JOIN (Select LEAD_ID,MAKE,MODEL,FUEL_TYPE FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS) ORDERS ON ORDERS.LEAD_ID = TF.APPOINTMENTID
LEFT JOIN (Select APPT_ID,C24_QUOTE from "PC_STITCH_DB"."FIVETRAN1_BI"."SALES_TRANSACTIONS") S ON ORDERS.LEAD_ID = S.APPT_ID )
where TRIP_STATUS not in ('Cancelled','Denied') and LEG_TYPE in ('Last_Mile','Middle_Mile','First_Mile') 
) where MONTH_NAME in ('Jul') 
"""   
cur.execute(query)
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

ws2=gc.open_by_url('https://docs.google.com/spreadsheets/d/1FrioxnnKCmLeqDTXKqqtt7i0VsQaF_wAjcCzgyebVZY/edit#gid=0').worksheet('Data1')
gd.set_with_dataframe(ws2,df,resize=False,row=1,col=1)

query="""

SELECT month_name,count(LEAD_ID) as pickup_done FROM (Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,REPLACE(PICKUP_CREATED,'.000','') AS PICKUP_CREATED,
REPLACE(FIRST_STOCKIN_DATE,'.000','') AS FIRST_STOCKIN_DATE,REPLACE(PICKUP_DATE_SYSTEM,'.000','') AS PICKUP_DATE_SYSTEM,TO_CHAR(PICKUP_DATE_SYSTEM,'MON-YYYY') AS month_name,
REPLACE(PICKUP_DATE_FINAL,'.000','') AS PICKUP_DATE_FINAL,
--PICKUP_TIME_SLOT_TO
RESCHEDULING_REASON
,STOCKIN_DATE,STORE_TYPE,PAYMENT_TYPE,
REPLACE(PAYMENT_DATE,'.000','') AS PAYMENT_DATE,STATUS_PICKUP,IS_DEAL_LOST,
--REPLACE(ASSIGNED_AT,'.000','') AS ASSIGNED_AT,
REPLACE(COMPLETE_ON,'.000','') AS COMPLETE_ON,REPLACE(RESCHEDULED_AT,'.000','') AS RESCHEDULED_AT,CONDITION,IS_PICKED,
--IN_TRANSIT_AT
TASK_ID,
REPLACE(LATEST_INSPECTION_DATE,'.000','') AS LATEST_INSPECTION_DATE,REPLACE(CUSTOMER_PICKUP_INTENT,'.000','') AS CUSTOMER_PICKUP_INTENT,
REPLACE(FIRST_INSPECTION_DATE,'.000','') AS FIRST_INSPECTION_DATE,DRIVER_TYPE,DEALER_CODE,REPLACE(TRACKER_LAST_UPDATED_AT,'.000','') AS TRACKER_LAST_UPDATED_AT,
GS_NON_GS_FLAG,IS_BREACH,SLOT,REPLACE(OPT_SENT,'.000','') AS OPT_SENT,REPLACE(OTP_VERIFIED_AT,'.000','') AS OTP_VERIFIED_AT,
REPLACE(OTP_SENT_DATE,'.000','') AS OTP_SENT_DATE,IS_BREACH_FLAG,IS_PLL_CASE

 from 
(Select T4.LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,PICKUP_CREATED,FIRST_STOCKIN_DATE,PICKUP_DATE_SYSTEM,PICKUP_DATE_FINAL,
PICKUP_TIME_SLOT_TO,STOCKIN_DATE,STORE_TYPE,PAYMENT_TYPE,PAYMENT_DATE,STATUS_PICKUP,ASSIGNED_AT,COMPLETE_ON,RESCHEDULED_AT,CONDITION,IS_PICKED,
IN_TRANSIT_AT,LATEST_INSPECTION_DATE,CUSTOMER_PICKUP_INTENT,FIRST_INSPECTION_DATE,DRIVER_TYPE,DEALER_CODE,Tracker_Last_Updated_At,GS_NON_GS_FLAG,IS_BREACH,
SLOT,OPT_SENT,OTP_VERIFIED_AT,OTP_SENT_DATE,IS_BREACH_FLAG,IS_PLL_CASE,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST
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
(Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,PICKUP_CREATED,FIRST_STOCKIN_DATE,
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
,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST
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

 from (Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY
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
,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST
--,PAYMENTHEADING
--,Diff
from
(Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,
PICKUP_CREATED,FIRST_STOCKIN_DATE,PICKUP_DATE_PICKUP,
Customer_Pickup_Intent,
PICKUP_TIME_SLOT_FROM,PICKUP_TIME_SLOT_TO,

to_date(FIRST_STOCKIN_DATE) as stockin_Date,
STORE_TYPE2 AS STORE_TYPE,PAYMENTHEADING,Payment_Date,STATUS_PICKUP,CANCELLED_AT,IN_TRANSIT_AT,ASSIGNED_AT,
COMPLETE_ON,IS_RESCHEDULED, RESCHEDULED_AT,Condition,INSPECTION_DATE,DRIVER_TYPE
,dealer_code,dealer_name
,cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ) Tracker_Last_Updated_At,
PICKUP_DATE2,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST

 from 
 (SELECT ORDERS.LEAD_ID,ORDERS.REGISTRATION_NUMBER,ORDERS.FUEL_TYPE,P.PARKING,
Case when UPPER(P.PARKING) like '%BANGALORE%' THEN 'BLR' ELSE P.REGION END as Parking_region,IFNULL(mu.store_name,Cl.centre) AS centre,
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
,s.dealer_code,s.dealer_name,OPS.RESCHEDULING_REASON,OPS.TASK_ID,

Case when orders.IS_DEAL_LOST_REQUEST = 0 then 'No'
when orders.IS_DEAL_LOST_REQUEST = 1 then 'Yes'
when orders.IS_DEAL_LOST_REQUEST = 2 then 'Pending At Finance' else 'NA' end as IS_DEAL_LOST

 
FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS

left join(select appointment_id,store_type as storetype,store_name from "PC_STITCH_DB"."DW"."MARKETING_UNIVERSE") mu on mu.appointment_id=orders.lead_id
LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_PICKUP_SERVICE OPS ON OPS.FK_ORDER_ID = ORDERS.ORDER_ID AND OPS.SERVICE_TYPE = 'PICKUP'

Left JOIN (SELECT * FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY FK_ORDER_ID ORDER by ORDER_INVENTORY_LOG_ID ASC) AS rank 
FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_INVENTORY_LOG 
WHERE ACTION = 'CHECKED_IN')A WHERE rank = 1)OLA ON OLA.FK_ORDER_ID = ORDERS.ORDER_ID

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
--TO_DATE(OLA.CREATED_AT) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-100 AND 
ORDERS.STATUS_ID <> 12 
--AND ORDERS.IS_DEAL_LOST_REQUEST = 0 
AND OPS.CREATED_AT IS NOT NULL --AND UPPER(CL.CENTRE) NOT LIKE ('%B2B%')
--AND UPPER(Cl.centre) NOT LIKE '%-PNS%' AND UPPER(Cl.centre) NOT LIKE '%- PNS%'
ORDER BY OLA.CREATED_AT ASC))T)T2
Where TO_DATE(pickup_date_final) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-100)T2
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
ON T4.LEAD_ID = PLL.LEAD_ID)FT) WHERE  GS_NON_GS_FLAG in ('GS') and  MONTH_NAME is not null and MONTH_NAME not in ('Jul-3021','Mar-2032')
group by 1


"""

cur.execute(query)
rows = cur.fetchall()
df1 = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

ws2=gc.open_by_url('https://docs.google.com/spreadsheets/d/1FrioxnnKCmLeqDTXKqqtt7i0VsQaF_wAjcCzgyebVZY/edit#gid=0').worksheet('pickup_data')
gd.set_with_dataframe(ws2,df1,resize=False,row=1,col=1)  

query="""

SELECT * FROM (Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,REPLACE(PICKUP_CREATED,'.000','') AS PICKUP_CREATED,
REPLACE(FIRST_STOCKIN_DATE,'.000','') AS FIRST_STOCKIN_DATE,REPLACE(PICKUP_DATE_SYSTEM,'.000','') AS PICKUP_DATE_SYSTEM,TO_CHAR(PICKUP_DATE_SYSTEM,'MON-YYYY') AS month_name,
REPLACE(PICKUP_DATE_FINAL,'.000','') AS PICKUP_DATE_FINAL,
--PICKUP_TIME_SLOT_TO
RESCHEDULING_REASON
,STOCKIN_DATE,STORE_TYPE,PAYMENT_TYPE,
REPLACE(PAYMENT_DATE,'.000','') AS PAYMENT_DATE,STATUS_PICKUP,IS_DEAL_LOST,
--REPLACE(ASSIGNED_AT,'.000','') AS ASSIGNED_AT,
REPLACE(COMPLETE_ON,'.000','') AS COMPLETE_ON,REPLACE(RESCHEDULED_AT,'.000','') AS RESCHEDULED_AT,CONDITION,IS_PICKED,
--IN_TRANSIT_AT
TASK_ID,
REPLACE(LATEST_INSPECTION_DATE,'.000','') AS LATEST_INSPECTION_DATE,REPLACE(CUSTOMER_PICKUP_INTENT,'.000','') AS CUSTOMER_PICKUP_INTENT,
REPLACE(FIRST_INSPECTION_DATE,'.000','') AS FIRST_INSPECTION_DATE,DRIVER_TYPE,DEALER_CODE,REPLACE(TRACKER_LAST_UPDATED_AT,'.000','') AS TRACKER_LAST_UPDATED_AT,
GS_NON_GS_FLAG,IS_BREACH,SLOT,REPLACE(OPT_SENT,'.000','') AS OPT_SENT,REPLACE(OTP_VERIFIED_AT,'.000','') AS OTP_VERIFIED_AT,
REPLACE(OTP_SENT_DATE,'.000','') AS OTP_SENT_DATE,IS_BREACH_FLAG,IS_PLL_CASE

 from 
(Select T4.LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,PICKUP_CREATED,FIRST_STOCKIN_DATE,PICKUP_DATE_SYSTEM,PICKUP_DATE_FINAL,
PICKUP_TIME_SLOT_TO,STOCKIN_DATE,STORE_TYPE,PAYMENT_TYPE,PAYMENT_DATE,STATUS_PICKUP,ASSIGNED_AT,COMPLETE_ON,RESCHEDULED_AT,CONDITION,IS_PICKED,
IN_TRANSIT_AT,LATEST_INSPECTION_DATE,CUSTOMER_PICKUP_INTENT,FIRST_INSPECTION_DATE,DRIVER_TYPE,DEALER_CODE,Tracker_Last_Updated_At,GS_NON_GS_FLAG,IS_BREACH,
SLOT,OPT_SENT,OTP_VERIFIED_AT,OTP_SENT_DATE,IS_BREACH_FLAG,IS_PLL_CASE,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST
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
(Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,PICKUP_CREATED,FIRST_STOCKIN_DATE,
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
,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST
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

 from (Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY
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
,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST
--,PAYMENTHEADING
--,Diff
from
(Select LEAD_ID,REGISTRATION_NUMBER,FUEL_TYPE,PARKING,PARKING_REGION,CENTRE,RETAIL_REGION,RETAIL_CITY,
PICKUP_CREATED,FIRST_STOCKIN_DATE,PICKUP_DATE_PICKUP,
Customer_Pickup_Intent,
PICKUP_TIME_SLOT_FROM,PICKUP_TIME_SLOT_TO,

to_date(FIRST_STOCKIN_DATE) as stockin_Date,
STORE_TYPE2 AS STORE_TYPE,PAYMENTHEADING,Payment_Date,STATUS_PICKUP,CANCELLED_AT,IN_TRANSIT_AT,ASSIGNED_AT,
COMPLETE_ON,IS_RESCHEDULED, RESCHEDULED_AT,Condition,INSPECTION_DATE,DRIVER_TYPE
,dealer_code,dealer_name
,cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ) Tracker_Last_Updated_At,
PICKUP_DATE2,RESCHEDULING_REASON,TASK_ID,IS_DEAL_LOST

 from 
 (SELECT ORDERS.LEAD_ID,ORDERS.REGISTRATION_NUMBER,ORDERS.FUEL_TYPE,P.PARKING,
Case when UPPER(P.PARKING) like '%BANGALORE%' THEN 'BLR' ELSE P.REGION END as Parking_region,IFNULL(mu.store_name,Cl.centre) AS centre,
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
,s.dealer_code,s.dealer_name,OPS.RESCHEDULING_REASON,OPS.TASK_ID,

Case when orders.IS_DEAL_LOST_REQUEST = 0 then 'No'
when orders.IS_DEAL_LOST_REQUEST = 1 then 'Yes'
when orders.IS_DEAL_LOST_REQUEST = 2 then 'Pending At Finance' else 'NA' end as IS_DEAL_LOST

 
FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDERS

left join(select appointment_id,store_type as storetype,store_name from "PC_STITCH_DB"."DW"."MARKETING_UNIVERSE") mu on mu.appointment_id=orders.lead_id
LEFT JOIN PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_PICKUP_SERVICE OPS ON OPS.FK_ORDER_ID = ORDERS.ORDER_ID AND OPS.SERVICE_TYPE = 'PICKUP'

Left JOIN (SELECT * FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY FK_ORDER_ID ORDER by ORDER_INVENTORY_LOG_ID ASC) AS rank 
FROM PC_STITCH_DB.ADMIN_PANEL_PROD_DEALERENGINE_PROD.ORDER_INVENTORY_LOG 
WHERE ACTION = 'CHECKED_IN')A WHERE rank = 1)OLA ON OLA.FK_ORDER_ID = ORDERS.ORDER_ID

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
--TO_DATE(OLA.CREATED_AT) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-100 AND 
ORDERS.STATUS_ID <> 12 
--AND ORDERS.IS_DEAL_LOST_REQUEST = 0 
AND OPS.CREATED_AT IS NOT NULL --AND UPPER(CL.CENTRE) NOT LIKE ('%B2B%')
--AND UPPER(Cl.centre) NOT LIKE '%-PNS%' AND UPPER(Cl.centre) NOT LIKE '%- PNS%'
ORDER BY OLA.CREATED_AT ASC))T)T2
Where TO_DATE(pickup_date_final) >= to_date(cast(convert_timezone('Asia/Kolkata',current_timestamp())as TIMESTAMP_NTZ))-60)T2
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
ON T4.LEAD_ID = PLL.LEAD_ID)FT) WHERE   MONTH_NAME not in ('Jul-3021','Mar-2032')

"""

cur.execute(query)
rows = cur.fetchall()
df2 = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

ws2=gc.open_by_url('https://docs.google.com/spreadsheets/d/1qv6BVhMkdpBOXxx85shj5jSKSVWs2Tu79flEVp0m-B8/edit?gid=0#gid=0').worksheet('Sheet1')
gd.set_with_dataframe(ws2,df2,resize=True,row=1,col=1)  