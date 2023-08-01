import json
import requests
import psycopg2
from datetime import datetime


import pandas as pd
import requests

# Make the GET request
response = requests.get('https://tiny-texts-jk9apq.s3.us-east-1.amazonaws.com/fdc57820-5a49-4467-b8ce-44e814948d0e.txt')

# Get the headers from the response
headers = response.headers

# Convert the headers (a dictionary) to a DataFrame
df = pd.DataFrame(headers.items(), columns=['Header', 'Value'])

# Print the DataFrame
print(df)



  
