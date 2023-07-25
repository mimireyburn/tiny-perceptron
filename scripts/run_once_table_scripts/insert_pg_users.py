import json
import psycopg2
from datetime import datetime

def insert_users():

    connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    connection.autocommit = True
    cursor = connection.cursor()

    with open('users.json') as f:
        users_data = json.load(f)

    for user in users_data:
        gender = user['gender'] if user['gender'] is not None else 'unkwn'
        country = user['country'] if user['country'] is not None else 'unkwn'
        cursor.execute(
            """
            INSERT INTO Users (id, age, country, gender) VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) 
            DO NOTHING
            """,
            (user['id'], user['age'], country, gender)
        )
    connection.commit()
    cursor.close()
    connection.close()
    print('users inserted successfully!')

insert_users()
  