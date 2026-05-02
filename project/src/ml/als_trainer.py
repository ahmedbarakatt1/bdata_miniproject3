import sys
import os
import time
import pickle
os.environ.setdefault("HADOOP_HOME", "D:/BigData/hadoop")
os.environ["PATH"] = os.environ.get("HADOOP_HOME", "D:/BigData/hadoop") + "/bin;" + os.environ.get("PATH", "")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F

from config.config import (SPARK_MASTER, SPARK_APP, DATA_PATH, MODEL_PATH,
                            GAME_TITLES_PATH, ALS_RANK, ALS_MAX_ITER, ALS_REG_PARAM,
                            RMSE_THRESHOLD, TOP_N)


def create_spark():
    return SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName(SPARK_APP) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.driver.extraJavaOptions", "-Xss8m") \
        .config("spark.executor.extraJavaOptions", "-Xss8m") \
        .getOrCreate()


def train_als(train_df, rank=ALS_RANK, maxIter=ALS_MAX_ITER, regParam=ALS_REG_PARAM):
    als = ALS(
        rank=rank,
        maxIter=maxIter,
        regParam=regParam,
        userCol="user_id",
        itemCol="item_id",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True
    )
    return als.fit(train_df)


def evaluate_model(model, test_df):
    predictions = model.transform(test_df)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    return evaluator.evaluate(predictions)


def tune_model(train_df, test_df):
    configs = [
        {"rank": ALS_RANK, "maxIter": ALS_MAX_ITER, "regParam": ALS_REG_PARAM},
        {"rank": 20, "maxIter": 10, "regParam": 0.1},
        {"rank": 10, "maxIter": 20, "regParam": 0.05},
        {"rank": 15, "maxIter": 15, "regParam": 0.01},
    ]
    print(f"{'Rank':<8}{'MaxIter':<10}{'RegParam':<12}{'RMSE':<10}")
    print("-" * 40)
    best_model = None
    best_rmse = float("inf")
    best_config = None
    for cfg in configs:
        model = train_als(train_df, cfg["rank"], cfg["maxIter"], cfg["regParam"])
        rmse = evaluate_model(model, test_df)
        print(f"{cfg['rank']:<8}{cfg['maxIter']:<10}{cfg['regParam']:<12.3f}{rmse:<10.4f}")
        if rmse < best_rmse:
            best_rmse = rmse
            best_model = model
            best_config = cfg
        if rmse <= RMSE_THRESHOLD:
            break
    print(f"\nBest config: {best_config}, RMSE: {best_rmse:.4f}")
    return best_model, best_rmse


def generate_recommendations(model, user_ids, n=TOP_N):
    game_titles = {}
    if os.path.exists(GAME_TITLES_PATH):
        with open(GAME_TITLES_PATH, "rb") as f:
            game_titles = pickle.load(f)
    item_map = {}
    if os.path.exists("data/raw/item_map.pkl"):
        with open("data/raw/item_map.pkl", "rb") as f:
            item_map = pickle.load(f)
    inverse_item_map = {v: k for k, v in item_map.items()}

    from pyspark.sql import SparkSession as SS
    spark_py = SS.getActiveSession()
    user_df = spark_py.createDataFrame([(uid,) for uid in user_ids], ["user_id"])
    recs = model.recommendForUserSubset(user_df, n)

    result = {}
    for row in recs.collect():
        uid = row["user_id"]
        items = []
        for rec in row["recommendations"]:
            iid = rec["item_id"]
            asin = inverse_item_map.get(iid, str(iid))
            title = game_titles.get(asin, f"Game_{asin}")
            items.append({"item_id": iid, "title": title, "predicted_rating": round(float(rec["rating"]), 3)})
        result[uid] = items
    return result


def measure_latency(model, user_ids):
    latencies = []
    for uid in user_ids:
        t0 = time.time()
        generate_recommendations(model, [uid], n=TOP_N)
        latencies.append(time.time() - t0)
    avg = sum(latencies) / len(latencies)
    print(f"Avg recommendation latency: {avg:.3f}s per user")
    if avg > 5:
        print("WARNING: Latency exceeds 5s target")
    return avg


if __name__ == "__main__":
    t_start = time.time()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    train_df = spark.read.parquet("data/processed/train.parquet").cache()
    test_df = spark.read.parquet("data/processed/test.parquet").cache()
    train_df.count()
    test_df.count()

    print("=== ALS Model Tuning ===")
    model, rmse = tune_model(train_df, test_df)
    print(f"\nFinal RMSE: {rmse:.4f}")

    model.write().overwrite().save(MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")

    sample_users = [row["user_id"] for row in train_df.select("user_id").distinct().limit(5).collect()]
    print("\n=== Sample Recommendations ===")
    recs = generate_recommendations(model, sample_users)
    for uid, items in recs.items():
        print(f"\nUser {uid}:")
        for item in items:
            print(f"  {item['title']} (predicted: {item['predicted_rating']})")

    print(f"\nTraining time: {time.time() - t_start:.1f}s")
    spark.stop()
