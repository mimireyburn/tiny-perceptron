# %%
import random
import httpx
import uuid
import time
# %%
headers={"User": str(random.randint(0, 9)), "Session": str(uuid.uuid4())}
raw_res = httpx.get("http://localhost:8080", headers=headers)
item_id = raw_res.text
print(item_id)
# %%
int(time.time())
