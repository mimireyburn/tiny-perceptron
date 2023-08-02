import psycopg2
import datetime
import redis

r = redis.Redis(host="localhost", port=6379, db=1, password="MvY4bQ7uN3")
connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")

def convert_none_to_default(value, default_value):
    return value if value is not None else default_value

with connection:
  cur = connection.cursor("sync_redis_cursor")
  cur.execute(
      "SELECT itc.item_key, i.created_at, i.user_id, i.type, itc.topics FROM items i INNER JOIN item_txt_content itc ON i.item_key = itc.item_key AND i.bucket_key = itc.bucket_key AND i.type = itc.type;"
  )
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
          item_key, created_at, user_id, item_type, topics = item

          item_key = convert_none_to_default(item_key, "Unknown")
          created_at = convert_none_to_default(created_at, "Unknown")
          user_id = convert_none_to_default(user_id, "Unknown")
          item_type = convert_none_to_default(item_type, "Unknown")
          topics = convert_none_to_default(topics, "Unknown")

          datetime_obj = datetime.datetime.strptime(str(created_at), "%Y-%m-%d %H:%M:%S")
          unix_timestamp = int(datetime_obj.timestamp())
          item_key_redis = f"item:{item_key}"
          # hmscript adds if does not exist
          pipe.hset(
              item_key_redis,
              mapping={
                  "id": str(item_key),
                  "user": int(user_id),
                  "type": str(item_type),
                  "unix": int(unix_timestamp),
                  "labels": str(topics),
              },
          )

      pipe.execute()
  cur.close()
