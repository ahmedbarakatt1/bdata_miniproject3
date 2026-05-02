# Real-Time Game Recommendation System

A real-time recommendation system for video games using Apache Kafka, Apache Spark (MLlib + Structured Streaming), and Streamlit.

**Team:** [Team Placeholder]

---

## Quick Start (5 commands)

```bash
scripts\start_kafka.bat
python scripts\download_dataset.py
python src\preprocessing\preprocess.py
python src\ml\als_trainer.py
streamlit run src\dashboard\dashboard.py
```

---

## Architecture Overview

```
Kafka Producer → Kafka (game-ratings) → Spark Streaming → Metrics/Alerts
                                                 ↓
ALS Model (trained on Amazon Reviews) → Recommendation Engine
                                                 ↓
                                         Streamlit Dashboard
```

---

## Component Table

| Component | File | Description |
|-----------|------|-------------|
| Config | `config/config.py` | All tunable parameters |
| Preprocessing | `src/preprocessing/preprocess.py` | Clean + split data |
| ALS Trainer | `src/ml/als_trainer.py` | Train + tune ALS model |
| Kafka Producer | `src/streaming/kafka_producer.py` | Simulate real-time stream |
| Spark Streaming | `src/streaming/spark_streaming.py` | Window analytics |
| Alert System | `src/alerts/alert_system.py` | Threshold-based alerts |
| Rec Engine | `src/integration/recommendation_engine.py` | ALS + streaming boost |
| Dashboard | `src/dashboard/dashboard.py` | Live Streamlit UI |

---

## Sample Output

```
=== Sample Recommendations ===
User 42:
  Call of Duty: Modern Warfare (predicted: 4.823)
  Halo 5: Guardians (predicted: 4.711)
  FIFA 22 (predicted: 4.650)

ALERT: Game 17 (FIFA 22) is trending — score 0.92
ALERT: User 42 activity spike — 30 interactions
```

---

## Dashboard Screenshot

Start the dashboard with:
```bash
streamlit run src/dashboard/dashboard.py
```
Navigate to `http://localhost:8501` in your browser.

---

## Requirements

- Python 3.8+
- Java 11 (OpenJDK)
- Apache Kafka 3.7.0
- Apache Spark 3.5.1
- See `requirements.txt` for Python packages
