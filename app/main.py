import confluent_kafka
import fastapi
import redis
import uuid
import json
import time

app = fastapi.FastAPI()


@app.on_event("startup")
async def startup_event():
    app.state.r = redis.Redis(host="redis", port=6379, db=0, password="MvY4bQ7uN3")
    app.state.k = confluent_kafka.Producer({"bootstrap.servers": "kafka:29092"})
    app.state.c = confluent_kafka.Consumer({"bootstrap.servers": "kafka:29092", "group.id": "pg-group-1", "auto.offset.reset": "latest"})
    app.state.c.subscribe(["recommender"])


@app.on_event("shutdown")
def shutdown_event():
    app.state.k.flush()


def process_message(msg):
    if msg is None or msg.error():
        print("Error while receiving Kafka message:", msg.error())
        return None

    try:
        # Parse the message value as JSON
        message_value = json.loads(msg.value())
        # Check if the message has the 'item_id' field
        if 'item_id' in message_value:
            return message_value['item_id']  # Return the 'item_id' value
        else:
            print("Received message without 'item_id' field, skipping...")
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON message: {e}")
    except KeyError as e:
        print(f"Message is missing 'item_id' field: {e}")
    return None



def get_data():
    msg = app.state.c.poll(1.0)
    return process_message(msg)


@app.get("/")
def read_root(request: fastapi.Request):
    app.state.r.incr("test_counter")
    user_id = request.headers.get("User")
    session = request.headers.get("Session")

    item_id = get_data()
    print("ITEM ID RETURNED FROM KAFKA: ", item_id)
    if item_id is None:
        return "No data available"

    ts = int(time.time())

    print(f"User {user_id} in session {session} requested an item at {ts}")

    # add to the user reco history of seen items
    # but also to session history to leverage in
    # session recommendations
    reco_info = {"item_id": item_id, "ts": ts}
    app.state.r.xadd(f"x:{user_id}", reco_info, maxlen=90, approximate=True)
    app.state.r.xadd(f"x:{user_id}:{session}", reco_info, maxlen=30, approximate=True)

    # package current interaction and just produce
    # it i.e. to send to kafka servers just yet,
    # get some more and and send it all together
    # every 5 interactions
    log_msg = json.dumps({"type": "reco", "user_id": user_id, "session": session, "item_id": item_id, "ts": ts})
    app.state.k.produce("logs", log_msg)
    app.state.k.flush()

    # finally return the item_id to the user
    return item_id


@app.post("/evt")
def get_evt(request: fastapi.Request):
    user_id = request.headers.get("User")
    session = request.headers.get("Session")
    ts = int(time.time())

    # add to session history to leverage in
    # session recommendations
    app.state.r.xadd(f"x:{user_id}:{session}", {"ts": ts}, maxlen=30, approximate=True)

    # package end session event and send it
    # to kafka servers directly, no batching
    log_msg = json.dumps({"type": "evt", "user_id": user_id, "session": session, "ts": ts})
    app.state.k.produce("logs", log_msg)
    app.state.k.flush()

    # just a string is good enough, 200
    return "ok"


@app.post("/item")
async def create_item(data: str = fastapi.Body(None)):
    app.state.r.set("test", data)
    return {"status": "success"}
