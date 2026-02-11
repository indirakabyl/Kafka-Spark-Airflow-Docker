import json
import time
import hashlib
import requests
from confluent_kafka import Producer

API_ENDPOINT = "https://randomuser.me/api/?results=1"
KAFKA_BOOTSTRAP_SERVERS = "kafka_broker_1:19092,kafka_broker_2:19093,kafka_broker_3:19094"
KAFKA_TOPIC = "names_topic"
PAUSE_INTERVAL = 10
STREAMING_DURATION = 120

def retrieve_user_data(url=API_ENDPOINT, retries=5, backoff=2):
    last_payload = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            payload = r.json()
            last_payload = payload

            results = payload.get("results")
            if isinstance(results, list) and results:
                return results[0]

            # empty results → retry
        except Exception as e:
            last_err = e

        time.sleep(backoff * attempt)

    # IMPORTANT: don't crash the DAG
    print(f"[WARN] randomuser returned empty results after {retries} retries. Last payload: {last_payload}")
    return None


    raise RuntimeError(f"Failed to fetch valid user data after {retries} retries: {last_err}")

def encrypt_zip(zip_code):
    zip_str = str(zip_code)
    return int(hashlib.md5(zip_str.encode()).hexdigest(), 16)

def transform_user_data(data: dict) -> dict:
    return {
        "name": f"{data['name']['title']}. {data['name']['first']} {data['name']['last']}",
        "gender": data["gender"],
        "address": f"{data['location']['street']['number']}, {data['location']['street']['name']}",
        "city": data["location"]["city"],
        "nation": data["location"]["country"],
        "zip": encrypt_zip(data["location"]["postcode"]),
        "latitude": float(data["location"]["coordinates"]["latitude"]),
        "longitude": float(data["location"]["coordinates"]["longitude"]),
        "email": data["email"],
    }

def configure_kafka():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "airflow-producer",
    })

def delivery_status(err, msg):
    if err:
        print("Message delivery failed:", err)
    else:
        print(f"Message delivered to {msg.topic()} [partition {msg.partition()}]")

def publish_to_kafka(producer, topic, data):
    producer.produce(topic, value=json.dumps(data).encode("utf-8"), callback=delivery_status)
    producer.flush()

def initiate_stream():
    producer = configure_kafka()
    for _ in range(STREAMING_DURATION // PAUSE_INTERVAL):
        raw = retrieve_user_data()
        if raw is None:
            time.sleep(PAUSE_INTERVAL)
            continue

        formatted = transform_user_data(raw)
        publish_to_kafka(producer, KAFKA_TOPIC, formatted)
        time.sleep(PAUSE_INTERVAL)

