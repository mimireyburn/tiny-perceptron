import json
import requests
import psycopg2
from datetime import datetime

def insert_items():

    connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    connection.autocommit = True
    cursor = connection.cursor()

    # Define the base URL and the step size
    base_url = "http://135.181.118.171:7070/items/"
    step = 20000

    # Loop over the API endpoints
    for i in range(0, 180001, step):

        # Make a request to the API endpoint
        response = requests.get(base_url + str(i))

        # If the request was successful, load the JSON data
        if response.status_code == 200:
            items_data = response.json()

            # Insert the data into the items table
            for item in items_data:
                created_at = datetime.strptime(item['created_at'], "%a, %d %b %Y %H:%M:%S %Z")
                cursor.execute(
                    """
                    INSERT INTO Items (bucket_key, created_at, item_key, type, user_id) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (item_key) 
                    DO NOTHING
                    """,
                    (item['bucket_key'], created_at, item['item_key'], item['type'], item['user_id'])
                )
            connection.commit()
            print('items inserted successfully from range {}-{}!'.format(i, i+step-1))

        else:
            print('Error: received status code {} from range {}-{}'.format(response.status_code, i, i+step-1))

    cursor.close()
    connection.close()

insert_items()

  
