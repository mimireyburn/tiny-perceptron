import requests
import psycopg2
from psycopg2 import sql



# ALTER TABLE public.item_txt_content ADD COLUMN IF NOT EXISTS "genre" text;
# ALTER TABLE public.item_txt_content ADD COLUMN IF NOT EXISTS "topics" text;
# ALTER TABLE public.item_txt_content ADD COLUMN IF NOT EXISTS "sentiment" text;
# ALTER TABLE public.item_txt_content ADD COLUMN IF NOT EXISTS "ctype" text;


def label_items():
    connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    connection.autocommit = True
    cursor = connection.cursor()

    # Insert the data into the items table
    cursor.execute(
            """
        SELECT i.item_key, i.bucket_key, i.type
        FROM public.items i
		left join item_txt_content itx on i.item_key = itx.item_key
        WHERE i.type = 'txt'
		and itx.topics is null
            """
        )# !This is just for TXT!
    rows = cursor.fetchall()
    
    domain= "s3.us-east-1.amazonaws.com"

    def get_item_content(bucket_key,domain,item_key):
        url = f"https://{bucket_key}.{domain}/{item_key}.txt"
        response = requests.get(url)
        if response.status_code == 200:
            content = response.content.decode('utf-8')

            # Fetch the header values
            ctype = response.headers.get('Content-Type')
            language = response.headers.get('x-amz-meta-language')
            sentiment = response.headers.get('x-amz-meta-sentiment')
            topics = response.headers.get('x-amz-meta-topics')
            genre = response.headers.get('x-amz-meta-genre')

            return content, ctype, language, sentiment, topics, genre
        else:
            return None, None, None, None, None, None

    for row in rows:
   
        item_key, bucket_key, _ = row
        # Fetch item content and the header values
        content, ctype, language, sentiment, topics, genre = get_item_content(bucket_key,domain,item_key)

    
        item_type = "txt"
        # Insert the content and the new properties into the table
        insert = sql.SQL("""
            INSERT INTO item_txt_content (item_key, bucket_key, type, url, content, ctype, language, sentiment, topics, genre)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (item_key) DO UPDATE SET
            bucket_key = EXCLUDED.bucket_key,
            type = EXCLUDED.type,
            url = EXCLUDED.url,
            content = EXCLUDED.content,
            ctype = EXCLUDED.ctype,
            language = EXCLUDED.language,
            sentiment = EXCLUDED.sentiment,
            topics = EXCLUDED.topics,
            genre = EXCLUDED.genre
        """)
        cursor.execute(insert, (item_key, bucket_key, item_type, f"https://{bucket_key}.{domain}/{item_key}.txt", content, ctype, language, sentiment, topics, genre))

    connection.commit()
    cursor.close()
    connection.close()

label_items()