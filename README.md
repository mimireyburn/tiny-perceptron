## Recommendation Engine: Team Perceptron Party

A competitive group ML project for the MLX2 Founders and Coders Level 7 AI Apprenticeship: Recommendation System for mock social-media app, TinyWorld.

## Architecture
<img width="772" alt="Screenshot 2023-07-20 at 13 32 02" src="https://github.com/mimireyburn/Perceptron/assets/36554605/2a122672-eff3-4878-8981-d01e271bb8bf">

Serices: 
- app
- db (postgres)
- cache (redis)
- kafka
- zookeeper
- workers

## Set-up

```sh
$ apt install -y python3.8-venv
$ python3.8 -m venv env
$ source env/bin/activate
$ pip install fastapi
$ pip install "uvicorn[standard]"
$ pip install redis
$ pip install psycopg2-binary
$ pip install confluent-kafka
# once done installing, save the packages
# in a "requirements.txt" file
$ pip freeze > requirements.txt
# next time they can be installed directly
$ pip install -r requirements.txt
```


```sh
$ uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
$ curl -X POST "http://localhost:8080/item" -H "Content-Type: application/json" -d '"Hello, Redis!"'
# {"status": "success"}
$ curl "http://localhost:8080"
# {"status": "Hello, Redis!"}
```


```sh
$ tilt up --host 0.0.0.0 --port 10351
```

## Services


```sh
$ open http://[HOST_IP_ADDRESS]:8002 # RedisInsight
$ open http://[HOST_IP_ADDRESS]:9080 # Kafka Dashboard
```

## Workbench

Visual Studio Code has a Jupyter extension which allows you to write
code directly on a python file and by adding `#%%` transform it in
an interactive cell. Try it out :)

## Scripts

```sh
$ source env/bin/activate
$ python scripts/create_pg_schema.py
```
