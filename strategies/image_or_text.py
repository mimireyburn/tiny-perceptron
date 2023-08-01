#create a sorted set for images and txt items ON STARTUP
all_keys = app.state.a.keys("item:*")
for key in all_keys:
    timestamp = app.state.a.hget(key, "unix")  # Assuming you store the timestamp in the "unix" field
    if app.state.a.hget(key, "type") == "img":
        app.state.a.zadd("img_sorted_set", {key: float(timestamp)})
    else:
        app.state.a.zadd("txt_sorted_set", {key: float(timestamp)})


#return a random image if strategy 0, txt if strategy 1 - IN /GET
reco_strategy = int(user_id) %2
if(reco_strategy == 0):
    random_item_key = app.state.a.zrandmember("img_sorted_set")
else:
    random_item_key = app.state.a.zrandmember("txt_sorted_set")

random_item_info = app.state.a.hgetall(random_item_key)
item_id = random_item_info['id']
ts = int(time.time())