import json
import psycopg2
from datetime import datetime

def insert_items():

    connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    connection.autocommit = True
    cursor = connection.cursor()

    with open('items0.json') as f:
        items_data = json.load(f)

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
    cursor.close()
    connection.close()
    print('items inserted successfully!')

insert_items()
  
