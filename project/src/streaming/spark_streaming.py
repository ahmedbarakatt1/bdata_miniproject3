import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

from config.config import (SPARK_MASTER, SPARK_APP, SPARK_KAFKA_PKG,
                            KAFKA_BOOTSTRAP, KAFKA_TOPIC,
                            WINDOW_SIZE, SLIDE_INTERVAL, WATERMARK, OUTPUT_PATH)

import math


def create_spark():
    return SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName(SPARK_APP) \
        .config("spark.jars.packages", SPARK_KAFKA_PKG) \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def read_stream(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()


def parse_stream(raw_df):
    schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("item_id", IntegerType()),
        StructField("rating", DoubleType()),
        StructField("timestamp", StringType())
    ])
    parsed = raw_df.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    parsed = parsed.filter(
        F.col("user_id").isNotNull() &
        F.col("item_id").isNotNull() &
        F.col("rating").isNotNull()
    )
    parsed = parsed.withColumn("event_time", F.to_timestamp(F.col("timestamp")))
    parsed = parsed.withWatermark("event_time", WATERMARK)
    return parsed


def compute_window_analytics(df):
    window_spec = F.window(F.col("event_time"), WINDOW_SIZE, SLIDE_INTERVAL)
    item_analytics = df.groupBy(window_spec, "item_id").agg(
        F.avg("rating").alias("avg_rating"),
        F.count("*").alias("interaction_count"),
        F.variance("rating").alias("rating_variance")
    )
    user_analytics = df.groupBy(window_spec, "user_id").agg(
        F.count("*").alias("interaction_count")
    )
    return item_analytics, user_analytics


def compute_trending_score(window_df):
    with_log = window_df.withColumn(
        "raw_trending",
        F.col("avg_rating") * F.log(F.col("interaction_count") + 1)
    )
    max_val = with_log.agg(F.max("raw_trending").alias("max_t")).collect()
    if max_val and max_val[0]["max_t"] and max_val[0]["max_t"] > 0:
        max_t = max_val[0]["max_t"]
        return with_log.withColumn("trending_score", F.col("raw_trending") / max_t)
    return with_log.withColumn("trending_score", F.lit(0.0))


def compute_engagement_score(window_df):
    max_val = window_df.agg(F.max("interaction_count").alias("max_ic")).collect()
    if max_val and max_val[0]["max_ic"] and max_val[0]["max_ic"] > 0:
        max_ic = max_val[0]["max_ic"]
        return window_df.withColumn(
            "engagement_score",
            F.col("interaction_count") / max_ic
        )
    return window_df.withColumn("engagement_score", F.lit(0.0))


def compute_rating_variance(df):
    window_spec = F.window(F.col("event_time"), WINDOW_SIZE, SLIDE_INTERVAL)
    return df.groupBy(window_spec, "item_id").agg(
        F.variance("rating").alias("rating_variance"),
        F.count("*").alias("count")
    )


def write_output(df, path, checkpoint):
    return df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", path) \
        .option("checkpointLocation", checkpoint) \
        .trigger(processingTime="10 seconds") \
        .start()


def run_streaming():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    raw_df = read_stream(spark)
    parsed_df = parse_stream(raw_df)

    item_analytics, user_analytics = compute_window_analytics(parsed_df)

    item_flat = item_analytics.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "item_id", "avg_rating", "interaction_count", "rating_variance"
    )
    user_flat = user_analytics.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "user_id", "interaction_count"
    )

    q1 = write_output(item_flat, OUTPUT_PATH + "metrics/items/", OUTPUT_PATH + "checkpoints/items/")
    q2 = write_output(user_flat, OUTPUT_PATH + "metrics/users/", OUTPUT_PATH + "checkpoints/users/")

    print("Streaming started. Waiting for data...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_streaming()
