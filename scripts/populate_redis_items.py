import psycopg2
import redis

def fetch_items_from_postgres():
    # PostgreSQL connection details
    pg_host = "postgres"
    pg_port = "5432"
    pg_database = "W9sV6cL2dX"
    pg_user = "root"
    pg_password = "E5rG7tY3fH"

    # Redis connection details
    redis_host = "redis"
    redis_port = "6379"
    redis_password = "MvY4bQ7uN3"

    try:
        # Connect to PostgreSQL
        pg_connection = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_database,
            user=pg_user,
            password=pg_password
        )
        pg_cursor = pg_connection.cursor()

        # Connect to Redis
        redis_connection = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True
        )

        # Fetch all items from PostgreSQL
        pg_cursor.execute("SELECT item_keys FROM items;")
        items = pg_cursor.fetchall()

        # Add items to Redis
        for item in items:
            redis_connection.sadd("items_set", item[0])

        print("Items successfully added to Redis!")

    except (psycopg2.Error, redis.exceptions.RedisError) as e:
        print(f"Error: {e}")

    finally:
        # Close connections
        if pg_connection:
            pg_connection.close()
        if redis_connection:
            redis_connection.close()

if __name__ == "__main__":
    fetch_items_from_postgres()
