import sys
import os
os.environ.setdefault("HADOOP_HOME", "D:/BigData/hadoop")
os.environ["PATH"] = os.environ.get("HADOOP_HOME", "D:/BigData/hadoop") + "/bin;" + os.environ.get("PATH", "")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.config import SPARK_MASTER, SPARK_APP, DATA_PATH


def create_spark():
    return SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName(SPARK_APP) \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .getOrCreate()


def load_data(spark):
    return spark.read.parquet(DATA_PATH)


def clean_data(df):
    df = df.dropna(subset=["user_id", "item_id", "rating", "timestamp"])
    df = df.filter((F.col("rating") >= 0.5) & (F.col("rating") <= 5.0))
    df = df.filter((F.col("user_id") > 0) & (F.col("item_id") > 0))
    window = Window.partitionBy("user_id", "item_id").orderBy(F.col("timestamp").desc())
    df = df.withColumn("rn", F.row_number().over(window)) \
           .filter(F.col("rn") == 1) \
           .drop("rn")
    return df


def get_stats(df):
    count = df.count()
    unique_users = df.select("user_id").distinct().count()
    unique_items = df.select("item_id").distinct().count()
    stats = df.select(
        F.mean("rating").alias("mean_rating"),
        F.stddev("rating").alias("std_rating"),
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).collect()[0]
    print(f"Records: {count:,}")
    print(f"Unique users: {unique_users:,}")
    print(f"Unique items: {unique_items:,}")
    print(f"Rating mean: {stats['mean_rating']:.3f}, std: {stats['std_rating']:.3f}")
    print(f"Date range: {stats['min_ts']} to {stats['max_ts']}")
    return count, unique_users, unique_items


def split_data(df):
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    return train_df, test_df


if __name__ == "__main__":
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    df = load_data(spark)
    df = clean_data(df)
    print("=== Dataset Statistics ===")
    get_stats(df)
    train_df, test_df = split_data(df)
    train_df.write.mode("overwrite").parquet("data/processed/train.parquet")
    test_df.write.mode("overwrite").parquet("data/processed/test.parquet")
    print(f"Train size: {train_df.count():,}, Test size: {test_df.count():,}")
    print("Saved train/test to data/processed/")
    spark.stop()
