import sys
import os
import json
import time
import random
import pickle
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import pandas as pd

from config.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, KAFKA_PARTITIONS


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8")
    )


def create_topic():
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        topic = NewTopic(name=KAFKA_TOPIC, num_partitions=KAFKA_PARTITIONS, replication_factor=1)
        admin.create_topics([topic])
        print(f"Topic '{KAFKA_TOPIC}' created with {KAFKA_PARTITIONS} partitions")
    except TopicAlreadyExistsError:
        print(f"Topic '{KAFKA_TOPIC}' already exists")
    except Exception as e:
        print(f"Topic creation warning: {e}")


def generate_event(user_ids, item_ids):
    user_id = random.choice(user_ids)
    item_id = random.choice(item_ids)
    rating = round(random.uniform(1.0, 5.0), 1)
    return {
        "user_id": int(user_id),
        "item_id": int(item_id),
        "rating": rating,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def run_producer(duration=300):
    create_topic()
    producer = create_producer()

    df = pd.read_parquet("data/raw/video_games_ratings.parquet")
    user_ids = df["user_id"].sample(min(1000, len(df))).tolist()
    item_ids = df["item_id"].sample(min(1000, len(df))).tolist()
    popular_game = item_ids[0]
    active_user = user_ids[0]

    start = time.time()
    sent = 0
    last_print = start

    while time.time() - start < duration:
        elapsed = time.time() - start

        if 60 <= elapsed < 61:
            for _ in range(50):
                event = generate_event(user_ids, [popular_game])
                key = event["user_id"] % KAFKA_PARTITIONS
                producer.send(KAFKA_TOPIC, key=key, value=event)
                sent += 1
            print(f"[{elapsed:.0f}s] Trending spike: sent 50 events for game {popular_game}")

        elif 120 <= elapsed < 121:
            for _ in range(30):
                event = generate_event([active_user], item_ids)
                key = active_user % KAFKA_PARTITIONS
                producer.send(KAFKA_TOPIC, key=key, value=event)
                sent += 1
            print(f"[{elapsed:.0f}s] User activity spike: sent 30 events for user {active_user}")

        else:
            event = generate_event(user_ids, item_ids)
            key = event["user_id"] % KAFKA_PARTITIONS
            producer.send(KAFKA_TOPIC, key=key, value=event)
            sent += 1
            time.sleep(0.5)

        if time.time() - last_print >= 10:
            print(f"[{time.time()-start:.0f}s] Sent {sent} events total")
            last_print = time.time()

    producer.flush()
    print(f"Producer done. Total sent: {sent}")


if __name__ == "__main__":
    run_producer(300)
