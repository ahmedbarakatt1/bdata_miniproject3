Work fully autonomously. Install anything needed. Download datasets automatically. Fix all errors and retry up to 5 times. Never ask for confirmation. Only stop if completely blocked.

=== PROJECT ===
Real-Time Recommendation System
Domain: Video Games (Amazon Reviews dataset)
Focus: Real-Time Intelligence — trending games, rating spikes, streaming analytics
Stack: Python + Apache Kafka + Apache Spark (MLlib + Structured Streaming) + Streamlit

=== STEP 1: ENVIRONMENT SETUP ===
Install system dependencies if missing: Java 11 (OpenJDK), Apache Spark 3.5.1, Apache Kafka 3.7.0.
Set SPARK_HOME and KAFKA_HOME environment variables.
Install Python packages:
pip install pyspark==3.5.1 kafka-python confluent-kafka pandas numpy scikit-learn matplotlib streamlit plotly requests tqdm pytest findspark

Create project structure:
project/
├── config/config.py
├── data/raw/
├── src/preprocessing/preprocess.py
├── src/ml/als_trainer.py
├── src/streaming/kafka_producer.py
├── src/streaming/spark_streaming.py
├── src/integration/recommendation_engine.py
├── src/alerts/alert_system.py
├── src/dashboard/dashboard.py
├── models/als_model/
├── output/recommendations/ output/alerts/ output/metrics/
├── tests/
├── scripts/start_kafka.bat start_kafka.sh
├── docs/report/report.md
├── requirements.txt
└── README.md

=== STEP 2: DATASET ===
Download Amazon Video Games reviews dataset:
URL: https://jmcauley.ucsd.edu/data/amazon_v2/categoryFiles/Video_Games.json.gz
Use Python requests + tqdm to download to data/raw/Video_Games.json.gz then decompress.
Each record is JSON with fields: reviewerID, asin, overall, unixReviewTime.
Parse and convert to DataFrame with columns: user_id (reviewerID), item_id (asin), rating (overall, float), timestamp (unixReviewTime, long).
Encode user_id and item_id to integer indices using LabelEncoder — ALS requires integer IDs.
Save mapping dicts to data/raw/user_map.pkl and data/raw/item_map.pkl for later decoding.
Also download metadata: https://jmcauley.ucsd.edu/data/amazon_v2/categoryFiles/metadata_Video_Games.json.gz
Parse to get asin → title mapping, save to data/raw/game_titles.pkl.
Verify: print total records (must be 500K+), unique users, unique items, rating distribution.

=== STEP 3: config/config.py ===
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

=== STEP 4: src/preprocessing/preprocess.py ===
load_data(spark): Load data/raw/video_games_ratings.parquet into Spark DataFrame.
clean_data(df): Drop nulls, filter rating 0.5–5.0, filter user_id>0 and item_id>0, drop duplicate (user_id, item_id) keeping latest timestamp.
get_stats(df): Print record count, unique users/items, rating mean/std, date range.
split_data(df): 80/20 random split. Return (train_df, test_df).
Main: run full pipeline, save train/test splits as parquet to data/processed/.

=== STEP 5: src/ml/als_trainer.py ===
train_als(train_df, rank, maxIter, regParam): ALS with userCol=user_id, itemCol=item_id, ratingCol=rating, coldStartStrategy=drop, nonnegative=True.
evaluate_model(model, test_df): Return RMSE using RegressionEvaluator.
tune_model(train_df, test_df): Train with default params. If RMSE > 1.5 try: (rank=20,iter=10,reg=0.1), (rank=10,iter=20,reg=0.05), (rank=15,iter=15,reg=0.01). Print tuning table. Return best model + RMSE.
generate_recommendations(model, user_ids, n=5): Return top-N as dict, decode item_id back to game title using game_titles.pkl.
measure_latency(model, user_ids): Time recommendation generation, print avg per user, flag if >5s.
Main: load data, tune, save model to models/als_model/, print RMSE + sample recommendations for 5 users.

=== STEP 6: src/streaming/kafka_producer.py ===
create_producer(): KafkaProducer with JSON serializer connected to KAFKA_BOOTSTRAP.
create_topic(): AdminClient creates topic "game-ratings" with 3 partitions if not exists. Partitioning strategy: user_id % 3 — same user always routes to same partition for ordering consistency.
generate_event(user_ids, item_ids): Return {user_id, item_id, rating: random 1.0–5.0 rounded to 1dp, timestamp: utcnow isoformat}.
run_producer(duration=300):
  - Load sample 1000 user_ids and item_ids from data/raw/video_games_ratings.parquet
  - 0–60s: normal traffic 2 events/sec
  - 60s: spike — send 50 events for one popular game (simulate trending)
  - 120s: user activity spike — 30 events from one user
  - Remainder: normal traffic
  - Print sent count every 10s
Main: run_producer(300)

=== STEP 7: src/streaming/spark_streaming.py ===
create_spark(): SparkSession with spark.jars.packages = SPARK_KAFKA_PKG.
read_stream(spark): Subscribe to KAFKA_TOPIC, return raw DataFrame.
parse_stream(raw_df):
  Schema: user_id INT, item_id INT, rating DOUBLE, timestamp STRING.
  Parse JSON from value column. Filter malformed (null) records.
  Add event_time = cast timestamp to TimestampType.
  Add watermark 15s on event_time. Late data policy: records >15s late are dropped.
compute_window_analytics(df): Window 30s slide 10s. Compute avg_rating per item_id and interaction_count per user_id.
compute_trending_score(window_df): trending_score = avg_rating * log(interaction_count+1), normalize 0–1 per window.
compute_engagement_score(window_df): engagement_score = interaction_count / max(interaction_count) per window.
compute_rating_variance(df): Rating variance per item per window.
write_output(df, path, checkpoint): Write stream to JSON with checkpointing.
run_streaming(): Wire everything, write analytics to output/metrics/, await termination.
Main: run_streaming()

=== STEP 8: src/alerts/alert_system.py ===
Functions called via foreachBatch on streaming output:
check_rating_alert(df): avg_rating > 4.5 → print + write "ALERT: Game {item_id} ({title}) has high avg rating {avg:.2f}" to output/alerts/rating_alerts.json
check_activity_spike(df): user interaction_count > 10 in window → "ALERT: User {user_id} activity spike — {n} interactions"
check_trending(df): trending_score > 0.8 → "ALERT: Game {item_id} is trending — score {score:.2f}"
All alerts include window_start, window_end, metric value, and alert_type field.

=== STEP 9: src/integration/recommendation_engine.py ===
load_model(): Load ALS model from models/als_model/.
get_historical_recs(model, user_id, n=5): Top-N from ALS with game titles decoded.
get_streaming_boost(item_ids, metrics_path): Read latest output/metrics/ JSON, return boost dict where trending_score > 0.8 items get +0.3 score boost.
generate_dynamic_recs(model, user_id, metrics_path, n=5):
  - Get historical recs
  - Apply streaming boosts to re-rank
  - Return final top-5 with game titles
  - Measure + print latency, flag if >5s
process_event(user_id, item_id, rating, model, metrics_path): Generate and print recs for incoming user.
run_loop(duration=60): Load model, every 5s pick random recent user, generate dynamic recs, print, track avg latency.

=== STEP 10: src/dashboard/dashboard.py ===
Streamlit app st.set_page_config(layout="wide"), title "Game Recommendation System — Live Dashboard".
Auto-refresh every 5s using st_autorefresh or manual refresh button.

Row 1 — 3 columns:
  Col 1 "Top Recommendations": load latest output/recommendations/, dropdown to select user_id, show top-5 games as table with title and predicted rating.
  Col 2 "Trending Games": load output/metrics/, plotly bar chart top-10 by trending_score, red bars if >0.8.
  Col 3 "User Activity": plotly line chart interaction_count over time for top-5 most active users.

Row 2 — full width "Live Alerts": load all output/alerts/ JSON, last 20 alerts as color-coded table (red=rating, orange=activity, yellow=trending).

Row 3 — 2 columns:
  Left "Avg Rating Over Time": line chart avg_rating per item for last 10 windows.
  Right "Rating Variance Heatmap": plotly heatmap items vs time, color=variance.

Run: streamlit run src/dashboard/dashboard.py

=== STEP 11: scripts/ ===
start_kafka.bat: Start Zookeeper, wait 5s, start Kafka, wait 5s, create topic with 3 partitions.
start_kafka.sh: Same for Linux/Mac.
run_all.bat: Start Kafka, then in separate terminals run producer, streaming, integration loop, and dashboard.

=== STEP 12: tests/ ===
tests/test_preprocessing.py: test schema validation, test cleaning removes nulls and out-of-range ratings, test 80/20 split ratio.
tests/test_als.py: test model trains on synthetic 200-row dataset, test RMSE is float 0–5, test recommendations return correct shape.
tests/test_streaming.py: test JSON parsing with mock data, test malformed records filtered, test windowed df has required columns.
Run pytest tests/ -v and fix all failures.

=== STEP 13: docs/report/report.md ===
Write complete project report with these sections:
1. System Architecture: ASCII diagram Kafka→Spark→ALS→Output→Dashboard, component descriptions, partitioning justification (user_id%3).
2. Dataset: Why Amazon Video Games — unique domain vs MovieLens, 500K+ records, distributed processing justification, cold-start and sparsity challenges.
3. ML: ALS explanation, preprocessing steps, RMSE result and tuning table with actual values, final params, sample recommendations.
4. Streaming: Kafka setup, window analytics justification (30s/10s), custom metrics formulas (trending, engagement, variance), late data policy (15s watermark = dropped).
5. Integration: Re-ranking algorithm, latency measurements, dynamic recommendation examples.
6. Alerts: All 3 alert types with trigger conditions and example outputs.
7. Dashboard: Description of all 5 visualizations.
8. Results: Fill with actual output after running.
9. Challenges and lessons learned.
10. How to run: exact commands for Windows and Linux.

=== STEP 14: README.md ===
Title, description, team placeholder, 5-command quick start, architecture overview, component table, sample output, dashboard screenshot instructions.

=== STEP 15: FINAL VALIDATION ===
Run in order, fix anything that fails:
1. pytest tests/ -v
2. python src/preprocessing/preprocess.py
3. python src/ml/als_trainer.py
4. Verify models/als_model/ is non-empty
5. python -c "from src.integration.recommendation_engine import generate_dynamic_recs; print('OK')"
6. python -c "import streamlit, plotly; print('Dashboard OK')"

Print final summary:
- RMSE achieved and whether tuning was needed
- Dataset size (users, items, ratings)
- Training time
- Sample recommendations for 3 users with game titles
- All tests passing confirmation

Then print:
PROJECT COMPLETE. Run order:
1. scripts/start_kafka.bat
2. python src/streaming/kafka_producer.py
3. python src/streaming/spark_streaming.py
4. python src/integration/recommendation_engine.py
5. streamlit run src/dashboard/dashboard.py