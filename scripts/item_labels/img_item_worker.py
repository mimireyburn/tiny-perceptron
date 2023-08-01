import requests
import psycopg2
from psycopg2 import sql

def label_img_items():
    connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute(
        """
        	
        SELECT i.item_key, i.bucket_key
        FROM public.items i
		left join item_img_content iti on i.item_key = iti.item_key
        WHERE i.type = 'img'
		and iti.topic is null
        """
    ) 
    rows = cursor.fetchall()

    domain= "s3.us-east-1.amazonaws.com"

    def get_img_item_label(bucket_key, domain, item_key):
        url = f"https://{bucket_key}.{domain}/{item_key}.jpg"
        response = requests.get(url)
        if response.status_code == 200:
            ctype = response.headers.get('Content-Type')
            adjective = response.headers.get('x-amz-meta-adjective')
            draw_type = response.headers.get('x-amz-meta-draw_type')
            category = response.headers.get('x-amz-meta-category')
            topic = response.headers.get('x-amz-meta-topic')

            return url, ctype, adjective, draw_type, category, topic

        else:
            return None, None, None, None, None, None
    for row in rows:
        item_key, bucket_key = row
        url, ctype, adjective, draw_type, category, topic = get_img_item_label(bucket_key,domain,item_key)

        insert_update = sql.SQL("""
            INSERT INTO item_img_content (item_key, bucket_key, type, url, ctype, adjective, draw_type, category, topic)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (item_key) DO UPDATE SET
            bucket_key = EXCLUDED.bucket_key,
            type = EXCLUDED.type,
            url = EXCLUDED.url,
            ctype = EXCLUDED.ctype,
            adjective = EXCLUDED.adjective,
            draw_type = EXCLUDED.draw_type, 
            category = EXCLUDED.category,
            topic = EXCLUDED.topic
        """)
        cursor.execute(insert_update, (item_key, bucket_key, 'img', url, ctype, adjective, draw_type, category, topic))
        

    connection.commit()
    cursor.close()
    connection.close()

label_img_items()
