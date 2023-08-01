#pip install psycopg2
import psycopg2

# Replace these variables with your PostgreSQL server connection details
host = '65.109.54.241'
port = '5432'
database = 'W9sV6cL2dX'
user = 'root'
password = 'E5rG7tY3fH'

# SQL statement to create the table and insert data
sql_statement = """
CREATE TABLE IF NOT EXISTS viewing_time (
  session_id UUID,
  item_id UUID,
  time_spent INTEGER
);

WITH example_with_next_evt AS (
  SELECT
    session_id::UUID,
    evnt_stamp,
    item_id::UUID,
    type,
    LEAD(evnt_stamp) OVER (PARTITION BY session_id ORDER BY evnt_stamp) AS next_evnt_stamp
  FROM public.fct_hourly_metric
)
INSERT INTO viewing_time (session_id, item_id, time_spent)
SELECT
  session_id,
  item_id,
  (next_evnt_stamp - evnt_stamp)::INTEGER AS time_spent
FROM example_with_next_evt
WHERE
  next_evnt_stamp IS NOT NULL;
"""

try:
    # Connect to the PostgreSQL server
    connection = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
    cursor = connection.cursor()

    # Execute the SQL statements
    cursor.execute(sql_statement)

    # Commit the changes to the database
    connection.commit()
    print("Table 'viewing_time' has been created and populated successfully.")

except Exception as e:
    print("Error:", e)

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()
