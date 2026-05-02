import sys
import os
import time
import json
import pickle
import random
import glob
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel

from config.config import (SPARK_MASTER, SPARK_APP, MODEL_PATH, GAME_TITLES_PATH,
                            OUTPUT_PATH, TOP_N, TRENDING_THRESHOLD, LATENCY_TARGET_S)


def create_spark():
    return SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName(SPARK_APP) \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()


def load_model():
    return ALSModel.load(MODEL_PATH)


def _load_titles():
    game_titles = {}
    if os.path.exists(GAME_TITLES_PATH):
        with open(GAME_TITLES_PATH, "rb") as f:
            game_titles = pickle.load(f)
    item_map = {}
    if os.path.exists("data/raw/item_map.pkl"):
        with open("data/raw/item_map.pkl", "rb") as f:
            item_map = pickle.load(f)
    inverse_item_map = {v: k for k, v in item_map.items()}
    return game_titles, inverse_item_map


def get_historical_recs(model, user_id, n=TOP_N):
    spark = SparkSession.getActiveSession()
    game_titles, inverse_item_map = _load_titles()
    user_df = spark.createDataFrame([(user_id,)], ["user_id"])
    recs = model.recommendForUserSubset(user_df, n)
    result = []
    for row in recs.collect():
        for rec in row["recommendations"]:
            iid = rec["item_id"]
            asin = inverse_item_map.get(iid, str(iid))
            title = game_titles.get(asin, f"Game_{asin}")
            result.append({
                "item_id": iid,
                "title": title,
                "score": round(float(rec["rating"]), 3)
            })
    return result


def get_streaming_boost(item_ids, metrics_path=OUTPUT_PATH + "metrics/items/"):
    boost = {}
    files = glob.glob(metrics_path + "*.json")
    if not files:
        return boost
    latest = sorted(files)[-1]
    try:
        with open(latest) as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                    iid = record.get("item_id")
                    score = record.get("trending_score", 0) or 0
                    if iid in item_ids and score > TRENDING_THRESHOLD:
                        boost[iid] = 0.3
                except:
                    continue
    except:
        pass
    return boost


def generate_dynamic_recs(model, user_id, metrics_path=OUTPUT_PATH + "metrics/items/", n=TOP_N):
    t0 = time.time()
    historical = get_historical_recs(model, user_id, n=n * 2)
    item_ids = [r["item_id"] for r in historical]
    boosts = get_streaming_boost(item_ids, metrics_path)
    for rec in historical:
        rec["score"] += boosts.get(rec["item_id"], 0)
    historical.sort(key=lambda x: x["score"], reverse=True)
    final = historical[:n]
    latency = time.time() - t0
    print(f"Dynamic recs for user {user_id}: latency {latency:.3f}s")
    if latency > LATENCY_TARGET_S:
        print(f"WARNING: Latency {latency:.3f}s exceeds {LATENCY_TARGET_S}s target")
    return final


def process_event(user_id, item_id, rating, model, metrics_path=OUTPUT_PATH + "metrics/items/"):
    print(f"Event: user={user_id}, item={item_id}, rating={rating}")
    recs = generate_dynamic_recs(model, user_id, metrics_path)
    print(f"Recommendations for user {user_id}:")
    for r in recs:
        print(f"  {r['title']} (score: {r['score']:.3f})")
    return recs


def run_loop(duration=60):
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    model = load_model()

    import pandas as pd
    df = pd.read_parquet("data/raw/video_games_ratings.parquet")
    user_ids = df["user_id"].unique().tolist()

    latencies = []
    start = time.time()
    while time.time() - start < duration:
        uid = random.choice(user_ids[:100])
        t0 = time.time()
        recs = generate_dynamic_recs(model, int(uid))
        latencies.append(time.time() - t0)
        time.sleep(5)

    avg_lat = sum(latencies) / len(latencies) if latencies else 0
    print(f"\nAvg latency over {len(latencies)} requests: {avg_lat:.3f}s")
    spark.stop()


if __name__ == "__main__":
    run_loop(60)
