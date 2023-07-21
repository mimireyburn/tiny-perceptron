import confluent_kafka
import prometheus_client
import psycopg2
import datetime
import json
import utils
import time 


utils.check_connection_status("postgres", 5432)
p = psycopg2.connect(host="postgres", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
producer = confluent_kafka.Producer({"bootstrap.servers": "kafka:29092"})

def pull_from_postgres():

  pull_query = """
    SELECT item_key
    FROM items
    ORDER BY RANDOM()
    LIMIT 1
    OFFSET 0
  """

  try:
    cursor = p.cursor()
    cursor.execute(pull_query)
    result = cursor.fetchone()
    cursor.close()
    p.close()
    print("Item pulled", result[0])
    if result:
      return result[0]
    else:
        return None
  except Exception as e:
    # PG_ERRORS.inc() # This is a Prometheus command
    print("Worker error: ", e)
    return None


def produce_to_topic(message):
    topic = "recommender"
    try:
        # Produce the message to the topic
        producer.produce(topic=topic, value=message)
        # Flush the producer to ensure the message is sent
        producer.flush()
        print(f"Message sent: {message}")
        time.sleep(5)
    except Exception as e:
        print(f"Failed to produce message: {e}")


def main():
    message = pull_from_postgres()
    if message is not None:
      produce_to_topic(message)
      print("Fetched and sent: ", message)
    else: 
      print("No data found!")


if __name__ == "__main__":
  # prometheus_client.start_http_server(9965)
  main()