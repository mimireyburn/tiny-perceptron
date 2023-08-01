import json
import requests
import psycopg2
from datetime import datetime

QUERY = """
SELECT 
    uy.user_id, 
    uy.gender,
    uy.country, 
    uy.age,
    sum(timespent) as TIMES,
    STRING_AGG(CONCAT(iti.topic, ':', uy.timespent::text), ', ') AS topic_timespent
    
FROM
    (SELECT 
        m.user_id,
        m.session_id, 
        m.item_id,
        m.timespent, 
        u.gender,
        u.country, 
        u.age
    FROM (
        SELECT 
            user_id,
            session_id,
            item_id,
            next_evt_unix - evnt_stamp AS Timespent
        FROM (
            SELECT 
                user_id,
                session_id, 
                evnt_stamp, 
                item_id, 
                type,
                LEAD(evnt_stamp) OVER (PARTITION BY session_id ORDER BY evnt_stamp) AS next_evt_unix
            FROM public.fct_hourly_metric
        ) a
        WHERE type = 'reco'
    ) m
    INNER JOIN users u ON m.user_id = u.id
    WHERE m.timespent > 2
    ) uy
INNER JOIN item_img_content iti ON iti.item_key = uy.item_id
GROUP BY uy.user_id, uy.gender, uy.country, uy.age
ORDER BY TIMES DESC;
"""

def string_to_dictionary(input_string):
    items_list = input_string.split(", ")
    result_dict = {}

    for item in items_list:
        item_name, timespent = item.split(":")
        result_dict[item_name.strip()] = int(timespent.strip())

    return result_dict


def insert_items():

    connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(QUERY)
    rows = cursor.fetchall()
    
    
    for row in rows:
        user_id = row[0]
        interests_string = row[-1]
        result_dict = string_to_dictionary(interests_string)
        json_object = json.dumps(result_dict)
        
        cursor.execute(
                """
                UPDATE users
                SET interests = %(interest)s
                WHERE id = %(user_id)s;
                """,
                {"interest":json_object, "user_id":user_id}
            )
        connection.commit()
    cursor.close()
    connection.close()

insert_items()





    # # Loop over the API endpoints
    # for i in range(0, 180001, step):

    #         # Insert the data into the items table
    #         for item in items_data:
    #             created_at = datetime.strptime(item['created_at'], "%a, %d %b %Y %H:%M:%S %Z")
    #             cursor.execute(
    #                 """
    #                 INSERT INTO Items (bucket_key, created_at, item_key, type, user_id) VALUES (%s, %s, %s, %s, %s)
    #                 ON CONFLICT (item_key) 
    #                 DO NOTHING
    #                 """,
    #                 (item['bucket_key'], created_at, item['item_key'], item['type'], item['user_id'])
    #             )
    #         connection.commit()
    #         print('items inserted successfully from range {}-{}!'.format(i, i+step-1))

    #     else:
    #         print('Error: received status code {} from range {}-{}'.format(response.status_code, i, i+step-1))



  
