import psycopg2
import datetime
import json
import utils

def calculate_viewing_metrics(start_time=None):
    # Connect to the PostgreSQL database
    utils.check_connection_status("postgres", 5432)
    p = psycopg2.connect(host="postgres", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    p.autocommit = True

    # Create the viewing_metrics table if it doesn't exist
    create_viewing_metrics_table = """
        CREATE TABLE IF NOT EXISTS viewing_metrics (
            user_id INTEGER,
            item_id UUID,
            start_time_for_viewing_item INTEGER,
            end_time_for_viewing_item INTEGER,
            time_spent_looking_at_item INTEGER
        );
    """
    try:
        cursor = p.cursor()
        cursor.execute(create_viewing_metrics_table)
        print("Viewing_metrics table created.")
    except Exception as e:
        print("Error creating viewing_metrics table", e)

    if not start_time:
        # Fetch the earliest evnt_stamp from the database as the automatic start_time
        fetch_earliest_evnt_stamp_query = "SELECT MIN(evnt_stamp) FROM fct_hourly_metric;"
        try:
            cursor = p.cursor()
            cursor.execute(fetch_earliest_evnt_stamp_query)
            result = cursor.fetchone()
            if result and result[0]:
                start_time = int(result[0].timestamp())
        except Exception as e:
            print("Error fetching the earliest evnt_stamp from the database", e)

    # Calculate and insert viewing metrics
    calculate_metrics_query = """
        INSERT INTO viewing_metrics (user_id, item_id, start_time_for_viewing_item, end_time_for_viewing_item, time_spent_looking_at_item)
        SELECT 
            curr.user_id,
            curr.item_id,
            curr.evnt_stamp AS start_time_for_viewing_item,
            next.evnt_stamp AS end_time_for_viewing_item,
            next.evnt_stamp - curr.evnt_stamp AS time_spent_looking_at_item
        FROM fct_hourly_metric curr
        JOIN fct_hourly_metric next
            ON curr.user_id = next.user_id
            AND curr.session_id = next.session_id
            AND curr.evnt_stamp < next.evnt_stamp
            AND curr.item_id IS NOT NULL
            AND (next.item_id IS NOT NULL OR next.evnt_stamp = (
                SELECT MIN(evnt_stamp) FROM fct_hourly_metric 
                WHERE user_id = next.user_id AND session_id = next.session_id
            ))
        WHERE curr.evnt_stamp >= {start_time};
    """
    
    try:
        cursor = p.cursor()
        cursor.execute(calculate_metrics_query)
        print("Viewing metrics calculated and inserted.")
    except Exception as e:
        print("Worker error", e)

    # Close the database connection
    p.close()

if __name__ == "__main__":
    # You can pass the desired start time in the format of a Unix timestamp
    # For example, to start from January 1, 2023, at 00:00:00, you can use:
    # start_time = int(datetime.datetime(2023, 1, 1).timestamp())
    # calculate_viewing_metrics(start_time=start_time)
    
    # If start_time is not specified, it will fetch the earliest evnt_stamp from the database as the starting point
    calculate_viewing_metrics()
