import os
os.environ.setdefault("HADOOP_HOME", "D:/BigData/hadoop")
os.environ.setdefault("hadoop.home.dir", "D:/BigData/hadoop")

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "game-ratings"
KAFKA_PARTITIONS = 3

SPARK_MASTER = "local[*]"
SPARK_APP = "GameRecommendationSystem"
SPARK_KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

DATA_PATH = "data/raw/video_games_ratings.parquet"
GAME_TITLES_PATH = "data/raw/game_titles.pkl"
MODEL_PATH = "models/als_model"
OUTPUT_PATH = "output/"

ALS_RANK = 10
ALS_MAX_ITER = 10
ALS_REG_PARAM = 0.1
RMSE_THRESHOLD = 1.5

WINDOW_SIZE = "30 seconds"
SLIDE_INTERVAL = "10 seconds"
WATERMARK = "15 seconds"

TOP_N = 5
TRENDING_THRESHOLD = 0.8
ALERT_RATING_THRESHOLD = 4.5
ALERT_ACTIVITY_THRESHOLD = 10
LATENCY_TARGET_S = 5
