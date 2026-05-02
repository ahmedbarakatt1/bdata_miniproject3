# Real-Time Game Recommendation System — Project Report

## 1. System Architecture

```
                    ┌─────────────────────────────────────────────────────┐
                    │                   SYSTEM OVERVIEW                    │
                    └─────────────────────────────────────────────────────┘

  [Amazon Reviews]                                          [Streamlit Dashboard]
       │                                                           ▲
       ▼                                                           │
  [Preprocessing]  ──►  [ALS Training]  ──►  [models/als_model/]  │
       │                                              │             │
       ▼                                              ▼             │
  [Kafka Producer] ──► [Kafka Topic: game-ratings]   │             │
       │                       │                      │             │
       │                       ▼                      │             │
       │              [Spark Structured Streaming]    │             │
       │                       │                      │             │
       │              ┌────────┴────────┐             │             │
       │              ▼                 ▼             │             │
       │        [Window Analytics]  [Alert System]   │             │
       │              │                 │             │             │
       │              ▼                 ▼             ▼             │
       │        [output/metrics/]  [output/alerts/]  │             │
       │                    │              │          │             │
       │                    └──────┬───────┘          │             │
       │                           ▼                  │             │
       └──────────────► [Recommendation Engine] ──────┴─────────────┘
                         (ALS + Streaming Boost)
```

**Components:**
- **Kafka Producer**: Simulates real-time review stream with normal traffic, trending spikes, and user activity spikes
- **Spark Structured Streaming**: Consumes Kafka stream, computes windowed analytics (trending, engagement, variance)
- **ALS Model**: Matrix factorization for collaborative filtering; trained offline on historical data
- **Recommendation Engine**: Combines ALS historical scores with streaming boosts for dynamic re-ranking
- **Alert System**: Fires alerts on high ratings (>4.5), user activity spikes (>10/window), and trending games (score>0.8)
- **Dashboard**: Streamlit live visualization with 5-second auto-refresh

**Partitioning justification**: `user_id % 3` ensures all events from the same user route to the same partition, preserving ordering consistency for per-user streaming analytics.

---

## 2. Dataset

**Why Amazon Video Games over MovieLens:**
- Unique domain with gaming-specific engagement patterns (hardware + software interactions)
- Temporal sparsity reflects real gaming behavior (burst purchases at release, long tail)
- Cold-start is more pronounced — many niche titles with <5 reviews

**Dataset facts:**
- Source: McAuley Lab Amazon Reviews v2
- Domain: Video Games (2000–2018)
- Size: 500K+ ratings (exact count in Section 8)
- Fields: reviewerID, asin, overall (1–5), unixReviewTime

**Distributed processing justification:** 500K+ records benefit from Spark's lazy evaluation and parallel partition processing — pandas would load all data into memory; Spark handles larger-than-memory datasets via partitioned reads.

**Challenges:**
- **Cold-start**: New users/items have no embedding — handled via `coldStartStrategy=drop` in ALS
- **Sparsity**: ~99% of user-item pairs unrated — ALS matrix factorization handles implicit patterns

---

## 3. ML: ALS Collaborative Filtering

**Algorithm:** Alternating Least Squares factorizes the R (user×item) matrix into U (user embeddings) × V (item embeddings) by alternately solving least-squares for U and V.

**Preprocessing:**
1. Drop nulls and filter ratings to [0.5, 5.0]
2. Filter user_id > 0 and item_id > 0 (valid encoded indices)
3. Deduplicate (user, item) pairs keeping latest timestamp
4. 80/20 random split for train/test

**Tuning table** (actual values populated after run):

| Rank | MaxIter | RegParam | RMSE |
|------|---------|----------|------|
| 10   | 10      | 0.100    | 1.7056 |
| 20   | 10      | 0.100    | 1.6890 |
| 10   | 20      | 0.050    | 1.7404 |
| 15   | 15      | 0.010    | 2.6064 |

**Final parameters:** rank=20, maxIter=10, regParam=0.1 (lowest RMSE: 1.6890).

**Sample recommendations:** See Section 8.

---

## 4. Streaming: Kafka + Spark Structured Streaming

**Kafka setup:**
- Topic: `game-ratings`, 3 partitions, replication factor 1
- Partitioning: `user_id % 3` (ordering guarantee per user)
- Serialization: JSON over UTF-8

**Window analytics justification:**
- **30-second window** captures enough events to compute meaningful averages (not too noisy)
- **10-second slide** provides fresh updates frequently without full recomputation
- This 3:1 ratio balances freshness vs. stability

**Custom metric formulas:**
```
trending_score   = avg_rating × log(interaction_count + 1), normalized to [0,1]
engagement_score = interaction_count / max(interaction_count) per window
rating_variance  = Var(rating) per item per window
```

**Late data policy:** Records arriving more than 15 seconds after their event_time are **dropped** via Spark watermarking. This ensures bounded state and prevents unbounded accumulation of stale data.

---

## 5. Integration: Dynamic Re-ranking

**Algorithm:**
1. Get top-N×2 historical ALS recommendations for the user
2. Read latest streaming metrics from `output/metrics/items/`
3. Apply +0.3 boost to any item with `trending_score > 0.8`
4. Re-sort by adjusted score, return top-N

**Latency measurements:** See Section 8 for actual values. Target: <5s per recommendation.

**Example output:** See Section 8.

---

## 6. Alerts

| Alert Type     | Trigger Condition               | Output                                        |
|----------------|---------------------------------|-----------------------------------------------|
| Rating Alert   | avg_rating > 4.5 in window      | `output/alerts/rating_alerts.json`            |
| Activity Spike | user interaction_count > 10/win | `output/alerts/activity_alerts.json`          |
| Trending       | trending_score > 0.8            | `output/alerts/trending_alerts.json`          |

**Example alert payloads:**
```json
{"alert_type": "rating", "item_id": 42, "title": "Halo 5", "avg_rating": 4.83, "window_start": "...", "window_end": "..."}
{"alert_type": "activity", "user_id": 7, "interaction_count": 30, "window_start": "...", "window_end": "..."}
{"alert_type": "trending", "item_id": 17, "title": "FIFA 22", "trending_score": 0.94, "window_start": "..."}
```

---

## 7. Dashboard

Five visualizations on the Streamlit dashboard:

1. **Top Recommendations** (Row 1, Col 1): User dropdown → top-5 game titles with predicted ratings
2. **Trending Games Bar Chart** (Row 1, Col 2): Top-10 items by trending score; red bars for score>0.8
3. **User Activity Line Chart** (Row 1, Col 3): interaction_count over time for top-5 active users
4. **Live Alerts Table** (Row 2): Last 20 alerts, color-coded (red=rating, orange=activity, yellow=trending)
5. **Avg Rating Over Time** (Row 3, Left): Line chart per item for last 10 windows
6. **Rating Variance Heatmap** (Row 3, Right): Items vs time windows, color=variance magnitude

Auto-refreshes every 5 seconds.

---

## 8. Results

- **Dataset size**: 826,752 users, 50,187 items, 1,324,710 ratings (after EOFError at 1.3M/2.5M — full dataset partially truncated during download)
- **Rating distribution**: 1★=152K, 2★=77K, 3★=124K, 4★=260K, 5★=709K (avg 3.979)
- **Final RMSE**: 1.6890 (tuning ran all 4 configs; best was rank=20)
- **Training time**: ~8 minutes on local[*] with 4GB driver memory
- **Avg recommendation latency**: <2s per user

**Sample recommendations (User 2040):**
1. SteelSeries QCK Gaming Mouse Pad - Portal2 Edition (4.944)
2. Hatsune Miku: Project Diva 2 PSP Game (4.615)
3. Game_B000BXJEB2 (4.568)
4. A Giant Collection of 10 Hidden Object Hits (4.566)
5. GBA 4-Player Link Cable (4.625)

---

## 9. Challenges and Lessons Learned

- **Windows + Spark**: Requires Java 11 on PATH and `SPARK_HOME` set; `findspark.init()` bridges Python/JVM
- **ALS cold-start**: `coldStartStrategy=drop` silently removes cold users from evaluation — real RMSE is on warm users only
- **Streaming state**: Watermarking must be set before aggregation; order matters in PySpark streaming DSL
- **Kafka on Windows**: Use `.bat` scripts from `%KAFKA_HOME%\bin\windows\`; ZooKeeper must start before broker
- **Parquet vs JSON**: Parquet outperforms JSON ~5× for columnar reads; critical for 500K+ row ALS training

---

## 10. How to Run

### Windows
```bat
# 1. Start Kafka
scripts\start_kafka.bat

# 2. Download dataset (first time only)
python scripts\download_dataset.py

# 3. Preprocess data
python src\preprocessing\preprocess.py

# 4. Train ALS model
python src\ml\als_trainer.py

# 5. Start producer (in new terminal)
python src\streaming\kafka_producer.py

# 6. Start Spark streaming (in new terminal)
python src\streaming\spark_streaming.py

# 7. Start integration loop (in new terminal)
python src\integration\recommendation_engine.py

# 8. Launch dashboard
streamlit run src\dashboard\dashboard.py
```

### Linux/Mac
```bash
bash scripts/start_kafka.sh
python scripts/download_dataset.py
python src/preprocessing/preprocess.py
python src/ml/als_trainer.py
python src/streaming/kafka_producer.py &
python src/streaming/spark_streaming.py &
python src/integration/recommendation_engine.py &
streamlit run src/dashboard/dashboard.py
```

### Run tests
```bash
cd project
pytest tests/ -v
```
