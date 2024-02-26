from gmail_functions import create_message_without_attachment
import pandas as pd

df = {'a': (1, 2, 3), 'b': (3, 6, 7)}
df1 = pd.DataFrame(df)
html=df1.to_html(index=False)

to=['abhishek.shukla@cars24.com']
cc=['abhishek.shukla@cars24.com','sahil.5@cars24.com','pritam.sain@cars24.com']
html=html = 'Report Data <br> PFA' +html
subject="Test Report12" 
create_message_without_attachment(to,cc,subject,html)
