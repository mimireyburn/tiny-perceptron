import requests
import psycopg2
from psycopg2 import sql

def label_items():
    connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS item_txt_content (
        item_key UUID PRIMARY KEY,
        bucket_key VARCHAR NOT NULL,
        type VARCHAR(5) NOT NULL,
        url TEXT NOT NULL,
        content TEXT
    )
"""
    ) 
    connection.commit()

    # Insert the data into the items table
    cursor.execute(
        """
        SELECT item_key, type, bucket_key
        FROM public.items
        WHERE type = 'txt'
        """
    ) # !This is just for TXT!
    rows = cursor.fetchall()

    domain= "s3.us-east-1.amazonaws.com"

    def get_item_content(bucket_key,domain,item_key,item_type):
        url = f"https://{bucket_key}.{domain}/{item_key}.{item_type}"
        response = requests.get(url)
        if response.status_code == 200:
            content = response.content.decode('utf-8')
            return content
        else:
            return None

    for row in rows:
        item_key, item_type, bucket_key = row
        # Send a GET request to the item's URL
        content = get_item_content(bucket_key,domain,item_key,item_type)

        # Insert the content into the new table
        insert = sql.SQL("""
            INSERT INTO item_txt_content (item_key, bucket_key, type, url, content)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (item_key) DO UPDATE SET
            bucket_key = EXCLUDED.bucket_key,
            type = EXCLUDED.type,
            url = EXCLUDED.url,
            content = EXCLUDED.content
        """)
        cursor.execute(insert, (item_key, bucket_key, item_type, f"https://{bucket_key}.{domain}/{item_key}.{item_type}", content))

    connection.commit()
    cursor.close()
    connection.close()

label_items()