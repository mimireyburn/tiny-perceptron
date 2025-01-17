import confluent_kafka
import fastapi
import redis
import json
import time


app = fastapi.FastAPI()

@app.on_event("startup")
async def startup_event():
    app.state.r = redis.Redis(host="redis", port=6379, db=0, password="MvY4bQ7uN3", decode_responses=True)
    app.state.a = redis.Redis(host="redis", port=6379, db=1, password="MvY4bQ7uN3", decode_responses=True)
    app.state.b = redis.Redis(host="redis", port=6379, db=2, password="MvY4bQ7uN3", decode_responses=True)
    app.state.k = confluent_kafka.Producer({"bootstrap.servers": "kafka:29092"})

@app.on_event("shutdown")
def shutdown_event():
    app.state.k.flush()


@app.get("/")
def read_root(request: fastapi.Request):
    app.state.r.incr("test_counter")
    user_id = request.headers.get("User")
    session = request.headers.get("Session")
    strategies=['preference', 'control']

    #return a random image if strategy 0, txt if strategy 1
    # reco_strategy = int(user_id) %2
    # if(reco_strategy == 0):
    user_preferences = app.state.b.hget(f"user:{user_id}", "interests")
    print("user preference", user_preferences)
    if user_preferences != "None":
        first_preference = user_preferences[1:-1].split(",")[0].split(":")[0]
        print("first preference", first_preference)
        random_item_key = f"item:{app.state.a.srandmember(first_preference[1:-1])}"
        print("itemkey", random_item_key)
        reco_strategy = 0
    else:
        random_item_key = app.state.a.randomkey()
        print("correct itemkey", random_item_key)
        reco_strategy = 1

    random_item_info = app.state.a.hgetall(random_item_key)
    print("info", random_item_info)
    item_id = random_item_info['id']
    ts = int(time.time())

    print(f"Sent user {user_id} in session {session} item {item_id} at {ts} with strategy {strategies[reco_strategy]}")

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
    log_msg = json.dumps({"type": "reco", "user_id": user_id, "session": session, "item_id": item_id, "ts": ts, "strategy": strategies[reco_strategy]})
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
    log_msg = json.dumps({"type": "evt", "user_id": user_id, "session": session, "ts": ts, "item_id": None, "strategy": None})
    app.state.k.produce("logs", log_msg)
    app.state.k.flush()

    # just a string is good enough, 200
    return "ok"


@app.post("/item")
async def create_item(data: str = fastapi.Body(None)):
    app.state.r.set("test", data)
    return {"status": "success"}
