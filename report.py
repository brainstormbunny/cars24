import snowflake.connector
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import warnings
import os
import imgkit
warnings.filterwarnings("ignore")
import gspread
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

home_directory = 'C:/Users/Cars24/Desktop/Notebook/'
gsheet_auth = f'{home_directory}sahil_creds.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)

def chunked(iterable, size):
    return [iterable[i:i+size] for i in range(0, len(iterable), size)]
sheet_url = "https://docs.google.com/spreadsheets/d/1muS_22SxRQ1rw_QRAILFwqySnSOzvUDEUtjEwxMUlzM/edit#gid=432951442"
sheet = gc.open_by_url(sheet_url)
worksheet = sheet.worksheet("CHD")
cell_range1 = worksheet.range("A2:T30")
data = [[cell.value for cell in row] for row in chunked(cell_range1, 20)]
data1 = pd.DataFrame(data)
data1.columns = data1.iloc[0]
data1 = data1.drop(data1.index[0]).reset_index(drop=True)
data1=data1.replace(np.nan,'')
print(data1)

html_table = data1.to_html(index=False)


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

html_file_path = 'C:/Users/Cars24/Desktop/test_report.html'

# Save the HTML content to a file
with open(html_file_path, 'w') as file:
    file.write(html_text)

print(f"HTML file saved successfully at: {html_file_path}")

wkhtmltoimage_path = 'C:/Program Files/wkhtmltopdf/bin/wkhtmltoimage.exe' 
folder_path = 'C:/Users/Cars24/Desktop/'
html_file_path = os.path.join(folder_path, 'test_report.html')
png_file_path = os.path.join(folder_path, 'test_report.png')
import os
import imgkit  # if you intend to use this library, make sure it's installed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def html_to_png(html_file, output_file):
    # Configure headless Chrome
    options = Options()
    options.add_argument("--headless")  
    driver = webdriver.Chrome(options=options)

    # Load HTML file
    driver.get("file:///" + html_file)

    driver.set_window_size(1280, 720)  # Set the window size according to your requirement

    # Capture the screenshot
    driver.save_screenshot(output_file)

    # Close the WebDriver
    driver.quit()

if __name__ == "__main__":
    folder_path = 'C:/Users/Cars24/Desktop/'
    html_file_path = os.path.join(folder_path, 'test_report.html')
    png_file_path = os.path.join(folder_path, 'test_report.png')

    html_to_png(html_file_path, png_file_path)
