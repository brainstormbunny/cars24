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
gsheet_auth = 'sahil_creds.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)

secret_key=os.environ['secret_key']
slack_token = secret_key
client = WebClient(token=slack_token)


sheet_url = 'https://docs.google.com/spreadsheets/d/144xWGvX7ipabfIkQUvIdzZY_wbwDLwzfjVLyoUmLvDA/edit?gid=871108598#gid=871108598'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("Slack Report")
cell_range1 = worksheet.range("D3:T22")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 17)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace(np.nan,'')

html_table = data1.to_html(escape=False, index=False)

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
        <h3>Business Report </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Business Report City Wise :</h4>
        {html_table}
    </div>
   
</body>
</html>
"""

html_file_path = 'report123.html'

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
    driver.set_window_size(1300, 1000)
    driver.save_screenshot(output_file)
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'report123.html'
    png_file_path = 'report123.png'

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


sheet_url = 'https://docs.google.com/spreadsheets/d/1f_cSxnwVkMoQ8yGaZzWKHmubsPLkMC8v2AxeGhTzV2A/edit?gid=233326270#gid=233326270'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("Sahil_Report")
cell_range1 = worksheet.range("A1:E10")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 5)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace(np.nan,'')

cell_range2 = worksheet.range("A14:E29")
data2 = [[cell.value for cell in row] for row in chunked(cell_range2, 5)]
data2 = pd.DataFrame(data2)
data2.columns = data2.iloc[0]
data2 = data2.drop(data2.index[0]).reset_index(drop=True)
data2=data2.replace(np.nan,'')

html_table1 = data1.to_html(escape=False, index=False)
html_table2 = data2.to_html(escape=False, index=False)

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
        <h3>6 MOB Report</h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>SM Wise :</h4>
        {html_table2}
    </div>

   </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>City Wise :</h4>
        {html_table1}
    </div>
</body>
</html>
"""

html_file_path = 'mob.html'

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
    
    driver.set_window_size(200, 1100)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'mob.html'
    png_file_path = 'mob.png'
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

#CHM
sheet_url = 'https://docs.google.com/spreadsheets/d/1YGqdfsKRDkyd0u-eQTNVMXpESRmF5e9T5So-UDQhBHg/edit?gid=597038663#gid=597038663'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("Mailer")
cell_range3 = worksheet.range("B9:G26")
data3 = [[cell.value for cell in row] for row in chunked(cell_range3, 6)]
data3 = pd.DataFrame(data3)
data3.columns = data3.iloc[0]
data3 = data3.drop(data3.index[0]).reset_index(drop=True)
data3=data3.replace(np.nan,'')
data3
html_table3 = data3.to_html(escape=False, index=False)

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
        <h3>CHM Pendency </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>CHM Pendency Dashboard:</h4>
        {html_table3}
    </div>
   
</body>
</html>
"""

html_file_path = 'chm.html'

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
    
    driver.set_window_size(200, 900)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'chm.html'
    png_file_path = 'chm.png'
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


#Sales Effectiveness Delivery Confirmation
sheet_url = 'https://docs.google.com/spreadsheets/d/1hooQkoEgoPMpdXjsaDsPDRzTC-z0kbCabMDEiKSzrwM/edit?gid=465329279#gid=465329279'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("Mailer")
cell_range4 = worksheet.range("B3:I20")
data4 = [[cell.value for cell in row] for row in chunked(cell_range4, 8)]
data4 = pd.DataFrame(data4)
data4.columns = data4.iloc[0]
data4 = data4.drop(data4.index[0]).reset_index(drop=True)
data4=data4.replace(np.nan,'')
data4

cell_range5 = worksheet.range("B23:I40")
data5 = [[cell.value for cell in row] for row in chunked(cell_range5, 8)]
data5 = pd.DataFrame(data5)
data5.columns = data5.iloc[0]
data5 = data5.drop(data5.index[0]).reset_index(drop=True)
data5=data5.replace(np.nan,'')
data5

html_table4 = data4.to_html(escape=False, index=False)
html_table5 = data5.to_html(escape=False, index=False)

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
        <h3> Pending Delivery Confirmation</h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Pending DC Cases Nov:</h4>
        {html_table4}
    </div>

   </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Pending DC Cases Oct:</h4>
        {html_table5}
    </div>
</body>
</html>
"""

html_file_path = 'Delivery_Confirmation.html'

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
    
    driver.set_window_size(750, 1320)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'Delivery_Confirmation.html'
    png_file_path = 'Delivery_Confirmation.png'
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

#Dealer_MI_Purchase_Request
sheet_url = 'https://docs.google.com/spreadsheets/d/1yi5_HYluzIAF41HNsToDr7rZIS3T7F43JGDG9sNhyOo/edit?gid=1335035320#gid=1335035320'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("Dashboard")
cell_range6 = worksheet.range("B5:E9")
data6 = [[cell.value for cell in row] for row in chunked(cell_range6, 4)]
data6 = pd.DataFrame(data6)
data6.columns = data6.iloc[0]
data6 = data6.drop(data6.index[0]).reset_index(drop=True)
data6=data6.replace(np.nan,'')
data6
#MI_Response_Dealer
sheet_url = 'https://docs.google.com/spreadsheets/d/1xnOXhLysnFQtQE7QmkbJ1gmA7V6kREvcbeOdPwKYVoU/edit?gid=148224652#gid=148224652'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("FTD / MTD Report")
cell_range7 = worksheet.range("B15:G20")
data7 = [[cell.value for cell in row] for row in chunked(cell_range7, 6)]
data7 = pd.DataFrame(data7)
data7.columns = data7.iloc[0]
data7 = data7.drop(data7.index[0]).reset_index(drop=True)
data7=data7.replace(np.nan,'')
data7
################################################################################################################################
#TNC CONGO MAIL GENERATION (Responses)
# sheet_url = 'https://docs.google.com/spreadsheets/d/1acODQOep-aCdwMIAi7oBXYAfQeLV8g30zMCW8kFYHAU/edit?gid=23821500#gid=23821500'
# sheet = gc.open_by_url(sheet_url)
# worksheet = sheet.worksheet("MTD / FTD Reports")
# cell_range9 = worksheet.range("B14:G19")
# data9 = [[cell.value for cell in row] for row in chunked(cell_range9, 6)]
# data9 = pd.DataFrame(data9)
# data9.columns = data9.iloc[0]
# data9 = data9.drop(data9.index[0]).reset_index(drop=True)
# data9=data9.replace(np.nan,'')
# data9


# html_table6 = data6.to_html(escape=False, index=False)
# html_table7 = data7.to_html(escape=False, index=False)
# html_table9 = data9.to_html(escape=False, index=False)


# html_template = f"""
# <!DOCTYPE html>
# <html lang="en">
# <head>
#     <meta charset="UTF-8">
#     <meta http-equiv="X-UA-Compatible" content="IE=edge">
#     <meta name="viewport" content="width=device-width, initial-scale=1.0">
#     <style>
#         body {{
#             width: fit-content;
#             font-family: 'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif;
#             align-content: center;
#         }}

#         table, th, td {{
#             border: 1px solid rgb(5, 9, 30);
#             border-collapse: collapse;
#             text-align: center;
#             text-indent: 5px;
#         }}

#         td {{
#             max-height: fit-content;
#             max-width: fit-content;
#         }}

#         #firstdiv {{
#             border-top: 2px solid rgb(76, 104, 65);
#             border-left: 2px solid rgb(76, 104, 65);
#             border-right: 2px solid rgb(76, 104, 65);
#             background-color: rgb(50, 91, 168);
#             background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
#             padding-bottom: 2px;
#             color: white;
#         }}

#         h3 {{
#             font-size: 35px;
#             margin-top: 10px;
#         }}

#         h4 {{
#             background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
#             text-align: left;
#             padding-left: 10px;
#             color: #ffffff;
#             background-color: rgb(5, 9, 30);
#             border: 2px solid rgb(5, 9, 30);
#         }}

#         th {{
#             background-image: linear-gradient(rgb(44, 60, 148), rgb(75, 17, 54));
#             color: #ffffff;
#         }}
#     </style>
# </head>
# <body style="display: inline-flexbox; align-content: center;">
#     <div id="firstdiv" style="text-align: center; background-color: black; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: auto;">
#         <h3>TNC Report & Summary</h3>
#     </div>
#     <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
#         <h4>TNC MTD & FTD Report:</h4>
#         {html_table9}
#     </div>
#    </div>
#     <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
#         <h4>MI Quotation MTD & FTD Report:</h4>
#         {html_table7}
#     </div>
#     </div>
#     <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
#         <h4>MI Purchase MTD and FTD Report :</h4>
#         {html_table6}
#     </div>
# </body>
# </html>
# """

# html_file_path = 'tnc.html'

# # Save the HTML content to a file
# with open(html_file_path, 'w') as file:
#     file.write(html_template)

# print(f"HTML file saved successfully at: {html_file_path}")


# def html_to_png(html_file, output_file):
#     # Configure headless Chrome
#     options = Options()
#     options.add_argument("--headless")  # Run in headless mode, i.e., without opening a browser window
#     driver = webdriver.Chrome(options=options)
    
#     driver.get("file:///" + os.path.abspath(html_file))
    
#     time.sleep(2) 
    
#     driver.set_window_size(350, 900)
    
#     driver.save_screenshot(output_file)
    
#     driver.quit()

# if __name__ == "__main__":
#     html_file_path = 'tnc.html'
#     png_file_path = 'tnc.png'
#     html_to_png(html_file_path, png_file_path)
# channel=['C06HR7PBTHP']
# for i  in channel:
#     image_path = png_file_path
#     channel=i
#     try:
#         response = client.files_upload(
#             channels=channel,
#             file=image_path,
#             title=f'''Report_
#             ''',
#             initial_comment=f'''            '''
#         )

#         if response['ok']:
#             print("Image sent successfully!")
#         else:
#             print("Failed to send image:", response['error'])

#     except SlackApiError as e:
#         print(f"Error sending image: {e.response['error']}")
##################################################################################################################
# #New_location_Launch_requirements_and_preparation
# sheet_url = 'https://docs.google.com/spreadsheets/d/1k1inLUwhes-hQh-r4l-QVfUTrn3qc-n5OBH_Qr7Kk7Q/edit?gid=1175537258#gid=1175537258'
# sheet = gc.open_by_url(sheet_url)
# worksheet = sheet.worksheet("Onboarding Dashboard")
# cell_range8 = worksheet.range("E3:I9")
# data8 = [[cell.value for cell in row] for row in chunked(cell_range8, 5)]
# data8 = pd.DataFrame(data8)
# data8.columns = data8.iloc[0]
# data8 = data8.drop(data8.index[0]).reset_index(drop=True)
# data8=data8.replace(np.nan,'')
# data8


#RC 
sheet_url = 'https://docs.google.com/spreadsheets/d/1KrdkdLgnmF0KFQlidZnwn9-VoiHfvgew4ejhhSj9vSs/edit?gid=0#gid=0'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("RC Format")
cell_range8 = worksheet.range("B17:K33")
data8 = [[cell.value for cell in row] for row in chunked(cell_range8, 10)]
data8 = pd.DataFrame(data8)
data8.columns = data8.iloc[0]
data8 = data8.drop(data8.index[0]).reset_index(drop=True)
data8=data8.replace(np.nan,'')
data8    

html_table8 = data8.to_html(escape=False, index=False)

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
        <h3>Dealer and DSA RTO Pendency	 </h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Dealer and DSA RTO Pendency:</h4>
        {html_table8}
    </div>
   
</body>
</html>
"""

html_file_path = 'Rc.html'

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
    
    driver.set_window_size(800, 900)
    
    driver.save_screenshot(output_file)
    
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'Rc.html'
    png_file_path = 'Rc.png'
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



#EW
sheet_url = 'https://docs.google.com/spreadsheets/d/1YAt0BAcF0_61UHsgu8FEBNaKnJr09JPEYZOqoFjl6kg/edit?gid=1368228566#gid=1368228566'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("EW Tracking")
cell_range10 = worksheet.range("C4:M20")
data10 = [[cell.value for cell in row] for row in chunked(cell_range10, 11)]
data10 = pd.DataFrame(data10)
data10.columns = data10.iloc[0]
data10 = data10.drop(data10.index[0]).reset_index(drop=True)
data10=data10.replace(np.nan,'')
data10

html_table10 = data10.to_html(escape=False, index=False)

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
        <h3>EW Tracking</h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>EW Tracking Report:</h4>
        {html_table10}
    </div>
   
</body>
</html>
"""

html_file_path = 'ew.html'

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
    driver.set_window_size(900, 780)
    driver.save_screenshot(output_file)
    driver.quit()

if __name__ == "__main__":
    html_file_path = 'ew.html'
    png_file_path = 'ew.png'
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


#Unnati
sheet_url = 'https://docs.google.com/spreadsheets/d/19MPDCK9TuA4psu0V3C97JgVFjOY72tqZoB4Ak91Pqqc/edit?gid=1218637369#gid=1218637369'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("MAIL DASHBOARD")
cell_range11 = worksheet.range("C51:T61")
data11 = [[cell.value for cell in row] for row in chunked(cell_range11, 18)]
data11 = pd.DataFrame(data11)
data11.columns = data11.iloc[0]
data11 = data11.drop(data11.index[0]).reset_index(drop=True)
data11=data11.replace(np.nan,'')


html_table11 = data11.to_html(escape=False, index=False)

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
        <h3>Unnati DCF Dashboard</h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Unnati:</h4>
        {html_table11}
    </div>
   
</body>
</html>
"""

html_file_path = 'unnati.html'

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
    driver.set_window_size(1200, 700)
    driver.save_screenshot(output_file)
    driver.quit()
if __name__ == "__main__":
    html_file_path = 'unnati.html'
    png_file_path = 'unnati.png'
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



#Partner_Maxx
sheet_url = 'https://docs.google.com/spreadsheets/d/1EMAzVgs-MDqClr3Xqgdu3HxkxEUi-RJ4yDUqffLhkPo/edit?gid=1172246457#gid=1172246457'
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("OVERALL")
cell_range12 = worksheet.range("A3:P17")
data12 = [[cell.value for cell in row] for row in chunked(cell_range12, 16)]
data12 = pd.DataFrame(data12)
data12.columns = data12.iloc[0]
data12 = data12.drop(data12.index[0]).reset_index(drop=True)
data12=data12.replace(np.nan,'')


html_table12 = data12.to_html(escape=False, index=False)

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
        <h3>Pmaxx Dashboard</h3>
    </div>
    <div class="city" style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: inline-flexbox; align-content: center;">
        <h4>Pmaxx:</h4>
        {html_table12}
    </div>
   
</body>
</html>
"""

html_file_path = 'pmax.html'

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
    driver.set_window_size(1200, 700)
    driver.save_screenshot(output_file)
    driver.quit()
if __name__ == "__main__":
    html_file_path = 'pmax.html'
    png_file_path = 'pmax.png'
    html_to_png(html_file_path, png_file_path)


# channel=['C06HR7PBTHP']
# for i  in channel:
#     image_path = png_file_path
#     channel=i
#     try:
#         response = client.files_upload(
#             channels=channel,
#             file=image_path,
#             title=f'''Report_
#             ''',
#             initial_comment=f'''            '''
#         )

#         if response['ok']:
#             print("Image sent successfully!")
#         else:
#             print("Failed to send image:", response['error'])

#     except SlackApiError as e:
#         print(f"Error sending image: {e.response['error']}")
