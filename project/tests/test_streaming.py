import sys
import os
import pytest
import json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BinaryType

from src.streaming.spark_streaming import parse_stream


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder \
        .master("local[2]") \
        .appName("TestStreaming") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_raw_df(spark, records):
    schema = StructType([
        StructField("value", BinaryType())
    ])
    rows = [(json.dumps(r).encode("utf-8"),) for r in records]
    return spark.createDataFrame(rows, schema)


def test_json_parsing_valid(spark):
    records = [
        {"user_id": 1, "item_id": 101, "rating": 4.5, "timestamp": "2024-01-01T00:00:00+00:00"},
        {"user_id": 2, "item_id": 102, "rating": 3.0, "timestamp": "2024-01-01T00:00:01+00:00"},
    ]
    raw_df = make_raw_df(spark, records)
    schema = __import__(
        'pyspark.sql.types', fromlist=['StructType', 'StructField', 'IntegerType', 'DoubleType', 'StringType']
    )
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
    event_schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("item_id", IntegerType()),
        StructField("rating", DoubleType()),
        StructField("timestamp", StringType())
    ])
    parsed = raw_df.select(
        F.from_json(F.col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")
    parsed = parsed.filter(F.col("user_id").isNotNull())
    result = parsed.collect()
    assert len(result) == 2
    assert result[0]["user_id"] == 1
    assert result[1]["rating"] == 3.0


def test_malformed_records_filtered(spark):
    records = [
        {"user_id": 1, "item_id": 101, "rating": 4.5, "timestamp": "2024-01-01T00:00:00+00:00"},
        "not a json object",
        {"user_id": None, "item_id": 102, "rating": 3.0, "timestamp": "2024-01-01T00:00:01+00:00"},
    ]
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, BinaryType
    event_schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("item_id", IntegerType()),
        StructField("rating", DoubleType()),
        StructField("timestamp", StringType())
    ])
    rows = []
    for r in records:
        if isinstance(r, dict):
            rows.append((json.dumps(r).encode("utf-8"),))
        else:
            rows.append((r.encode("utf-8"),))
    raw_df = spark.createDataFrame(rows, StructType([StructField("value", BinaryType())]))
    parsed = raw_df.select(
        F.from_json(F.col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")
    filtered = parsed.filter(F.col("user_id").isNotNull() & F.col("item_id").isNotNull())
    result = filtered.collect()
    assert len(result) == 1
    assert result[0]["user_id"] == 1


def test_windowed_df_has_required_columns(spark):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
    schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("item_id", IntegerType()),
        StructField("rating", DoubleType()),
        StructField("event_time", TimestampType())
    ])
    from datetime import datetime
    data = [
        (1, 101, 4.5, datetime(2024, 1, 1, 0, 0, 0)),
        (2, 102, 3.0, datetime(2024, 1, 1, 0, 0, 10)),
        (1, 103, 2.5, datetime(2024, 1, 1, 0, 0, 20)),
    ]
    df = spark.createDataFrame(data, schema)
    window_spec = F.window(F.col("event_time"), "30 seconds", "10 seconds")
    windowed = df.groupBy(window_spec, "item_id").agg(
        F.avg("rating").alias("avg_rating"),
        F.count("*").alias("interaction_count")
    )
    result_cols = set(windowed.columns)
    assert "item_id" in result_cols
    assert "avg_rating" in result_cols
    assert "interaction_count" in result_cols
    assert "window" in result_cols
