import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import time
import os
import datetime as dt
from datetime import datetime
import gspread_dataframe as gd
from httplib2 import Http
from json import dumps
from datetime import datetime, timedelta
import imaplib
import shutil
import email
import pytz
import warnings
warnings.simplefilter("ignore")
today = datetime.strftime(datetime.now()-dt.timedelta(days=0),'%Y-%m-%d')
current_date = datetime.strftime(datetime.now()-dt.timedelta(days=0),'%Y-%m-%d')
last_3_days = datetime.strftime(datetime.now()-dt.timedelta(days=3),'%Y-%m-%d')
datestring_d0 = datetime.strftime(datetime.now(),'%Y-%m-%d_%H:%M:%S')



folder_to_clear1 = 'C:/Users/Cars24/Desktop/Notebook/attachments/'
folder_to_clear2 = 'C:/Users/Cars24/Desktop/Notebook/extracted/'

def clear_folder(folder_path):
    if os.path.exists(folder_path):
        # Remove all files and subdirectories in the folder
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            if os.path.isfile(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
        print(f'Folder "{folder_path}" has been cleared.')
    else:
        print(f'Folder "{folder_path}" does not exist.')


clear_folder(folder_to_clear1)
clear_folder(folder_to_clear2)




detach_dir = 'C:/Users/Cars24/Desktop/Notebook/'
if 'attachments' not in os.listdir(detach_dir):
    os.mkdir(os.path.join(detach_dir, 'attachments'))

userName = 'sahil.5@cars24.com'
passwd = 'qcig mwam pmgk seys'



# Create an instance of datetime to represent the current date and time in the Indian Standard Time (IST) timezone
timezone = pytz.timezone('Asia/Kolkata')  # Use 'Asia/Kolkata' for IST
now = datetime.now(timezone)

# Calculate the start time as 6 hours ago
start_time = now - timedelta(hours=24)

# Format the start and end times as 'dd-MMM-yyyy HH:mm:ss'
since_date = start_time.strftime('%d-%b-%Y %H:%M:%S')
until_date = now.strftime('%d-%b-%Y %H:%M:%S')

print(f'since_date: {since_date}')
print(f'until_date: {until_date}')

try:
    imapSession = imaplib.IMAP4_SSL('imap.gmail.com')
    typ, accountDetails = imapSession.login(userName, passwd)
    if typ != 'OK':
        print('Not able to sign in!')
        raise
    
    typ, response = imapSession.select('INBOX')  # Select the "INBOX" folder
    if typ != 'OK':
        print(f'Error selecting inbox: {response}')
        raise
    
    # Construct the search criteria for emails with the subject
    search_criteria = f'SUBJECT "Task report "'
    typ, data = imapSession.search(None, search_criteria)
    if typ != 'OK':
        print('Error searching Inbox.')
        raise
    
    for num in data[0].split():
        typ, msg_data = imapSession.fetch(num, '(RFC822)')
        if typ != 'OK':
            print('Error fetching mail.')
            continue

        emailBody = msg_data[0][1]
        mail = email.message_from_bytes(emailBody)
        
        # Check the date of the email
        email_date = mail.get('Date')
        # Remove the timezone information (e.g., (IST))
        email_date = email_date.replace('(IST)', '').strip()
        email_date = datetime.strptime(email_date, "%a, %d %b %Y %H:%M:%S %z")
        
        # Check if the email's date is within the last 6 hours
        if start_time <= email_date <= now:
            for part in mail.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                fileName = part.get_filename()

                if bool(fileName):
                    filePath = os.path.join(detach_dir, 'attachments', fileName)
                    if not os.path.isfile(filePath):
                        with open(filePath, 'wb') as fp:
                            fp.write(part.get_payload(decode=True))
                        print(f'Saved: {fileName}')
    
    imapSession.close()
    imapSession.logout()
except Exception as e:
    print(f'Error: {e}')



