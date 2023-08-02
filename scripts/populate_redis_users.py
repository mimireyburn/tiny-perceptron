import psycopg2
import redis

r = redis.Redis(host="localhost", port=6379, db=2, password="MvY4bQ7uN3")
connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")

print("Add users1...")

def convert_none_to_default(value, default_value):
    return value if value is not None else default_value

with connection:
    cur = connection.cursor("sync_redis_cursor")
    cur.execute("SELECT id, gender, country, age, interests FROM users;")
    batch_size = 5000
    curr_counter = 0

    while True:
        print("BATCH:", curr_counter)
        items = cur.fetchmany(batch_size)
        if not items:
            break
        curr_counter += 1
        pipe = r.pipeline()

        for item in items:
            user_id, gender, country, age, interests = item
            item_key_redis = f"user:{user_id}"

            interests = str(interests)

            # Convert NoneType values to appropriate types and handle age as "Unknown"
            gender = convert_none_to_default(gender, "Unknown")
            country = convert_none_to_default(country, "Unknown")
            age = convert_none_to_default(age, "Unknown")
            interests = convert_none_to_default(interests, "Unknown")

            pipe.hset(item_key_redis, mapping={
                "user_id": user_id,
                "gender": gender,
                "country": country,
                "age": age,
                "interests": interests
            })
        pipe.execute()
    cur.close()
