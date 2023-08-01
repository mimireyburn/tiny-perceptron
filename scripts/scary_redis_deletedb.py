import redis

# Connect to Redis
r = redis.Redis(host="localhost", port=6379, db=2, password="MvY4bQ7uN3")

# Flush the Redis database 1
r.flushdb()

print("Redis database 2 has been wiped.")
