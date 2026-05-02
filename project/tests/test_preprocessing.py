import sys
import os
import pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

from src.preprocessing.preprocess import clean_data, split_data


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder \
        .master("local[2]") \
        .appName("TestPreprocessing") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def sample_df(spark):
    schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("item_id", IntegerType()),
        StructField("rating", DoubleType()),
        StructField("timestamp", LongType())
    ])
    data = [
        (1, 101, 4.5, 1000),
        (2, 102, 3.0, 2000),
        (3, 103, None, 3000),
        (4, 104, 6.0, 4000),
        (5, 105, 0.0, 5000),
        (0, 106, 2.5, 6000),
        (7, 0, 3.5, 7000),
        (8, 108, 4.0, 8000),
        (8, 108, 4.5, 9000),
    ]
    return spark.createDataFrame(data, schema)


def test_schema_validation(sample_df):
    cols = set(sample_df.columns)
    assert "user_id" in cols
    assert "item_id" in cols
    assert "rating" in cols
    assert "timestamp" in cols


def test_cleaning_removes_nulls_and_invalid(sample_df):
    cleaned = clean_data(sample_df)
    rows = cleaned.collect()
    ratings = [r["rating"] for r in rows]
    user_ids = [r["user_id"] for r in rows]
    item_ids = [r["item_id"] for r in rows]
    assert None not in ratings
    assert all(0.5 <= r <= 5.0 for r in ratings)
    assert all(u > 0 for u in user_ids)
    assert all(i > 0 for i in item_ids)


def test_cleaning_removes_duplicates(sample_df):
    cleaned = clean_data(sample_df)
    pairs = [(r["user_id"], r["item_id"]) for r in cleaned.collect()]
    assert len(pairs) == len(set(pairs))


def test_split_ratio(spark):
    schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("item_id", IntegerType()),
        StructField("rating", DoubleType()),
        StructField("timestamp", LongType())
    ])
    data = [(i, i + 100, float(i % 5 + 1), i * 1000) for i in range(1, 101)]
    df = spark.createDataFrame(data, schema)
    train, test = split_data(df)
    total = df.count()
    train_count = train.count()
    test_count = test.count()
    assert abs(train_count / total - 0.8) < 0.1
    assert abs(test_count / total - 0.2) < 0.1
