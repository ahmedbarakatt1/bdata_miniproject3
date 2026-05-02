import sys
import os
import pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

from src.ml.als_trainer import train_als, evaluate_model, generate_recommendations


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder \
        .master("local[2]") \
        .appName("TestALS") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def synthetic_data(spark):
    schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("item_id", IntegerType()),
        StructField("rating", DoubleType()),
        StructField("timestamp", LongType())
    ])
    import random
    random.seed(42)
    data = [
        (random.randint(1, 20), random.randint(1, 30), float(random.randint(1, 5)), i)
        for i in range(200)
    ]
    return spark.createDataFrame(data, schema)


def test_model_trains(synthetic_data):
    train, test = synthetic_data.randomSplit([0.8, 0.2], seed=42)
    model = train_als(train, rank=5, maxIter=5, regParam=0.1)
    assert model is not None


def test_rmse_is_valid(synthetic_data):
    train, test = synthetic_data.randomSplit([0.8, 0.2], seed=42)
    model = train_als(train, rank=5, maxIter=5, regParam=0.1)
    rmse = evaluate_model(model, test)
    assert isinstance(rmse, float)
    assert 0 <= rmse <= 5.0


def test_recommendations_shape(synthetic_data, spark):
    train, _ = synthetic_data.randomSplit([0.8, 0.2], seed=42)
    model = train_als(train, rank=5, maxIter=5, regParam=0.1)
    user_ids = [row["user_id"] for row in train.select("user_id").distinct().limit(3).collect()]
    recs = generate_recommendations(model, user_ids, n=3)
    assert isinstance(recs, dict)
    for uid in recs:
        assert len(recs[uid]) <= 3
        for item in recs[uid]:
            assert "item_id" in item
            assert "title" in item
            assert "predicted_rating" in item
