import sys
import os
import json
import pickle
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from config.config import (ALERT_RATING_THRESHOLD, ALERT_ACTIVITY_THRESHOLD,
                            TRENDING_THRESHOLD, GAME_TITLES_PATH, OUTPUT_PATH)


def _load_game_titles():
    if os.path.exists(GAME_TITLES_PATH):
        with open(GAME_TITLES_PATH, "rb") as f:
            return pickle.load(f)
    return {}


def _load_item_map():
    path = "data/raw/item_map.pkl"
    if os.path.exists(path):
        with open(path, "rb") as f:
            item_map = pickle.load(f)
        return {v: k for k, v in item_map.items()}
    return {}


def _write_alert(alert, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(alert) + "\n")


def check_rating_alert(df):
    game_titles = _load_game_titles()
    inverse_item_map = _load_item_map()
    alerts = []
    for row in df.collect():
        avg = row.get("avg_rating") if isinstance(row, dict) else getattr(row, "avg_rating", None)
        item_id = row.get("item_id") if isinstance(row, dict) else getattr(row, "item_id", None)
        if avg and avg > ALERT_RATING_THRESHOLD:
            asin = inverse_item_map.get(item_id, str(item_id))
            title = game_titles.get(asin, f"Game_{asin}")
            alert = {
                "alert_type": "rating",
                "item_id": item_id,
                "title": title,
                "avg_rating": round(float(avg), 2),
                "window_start": str(getattr(row, "window_start", "")),
                "window_end": str(getattr(row, "window_end", "")),
                "created_at": datetime.utcnow().isoformat()
            }
            msg = f"ALERT: Game {item_id} ({title}) has high avg rating {avg:.2f}"
            print(msg)
            _write_alert(alert, OUTPUT_PATH + "alerts/rating_alerts.json")
            alerts.append(alert)
    return alerts


def check_activity_spike(df):
    alerts = []
    for row in df.collect():
        count = row.get("interaction_count") if isinstance(row, dict) else getattr(row, "interaction_count", None)
        user_id = row.get("user_id") if isinstance(row, dict) else getattr(row, "user_id", None)
        if count and count > ALERT_ACTIVITY_THRESHOLD:
            alert = {
                "alert_type": "activity",
                "user_id": user_id,
                "interaction_count": int(count),
                "window_start": str(getattr(row, "window_start", "")),
                "window_end": str(getattr(row, "window_end", "")),
                "created_at": datetime.utcnow().isoformat()
            }
            msg = f"ALERT: User {user_id} activity spike — {count} interactions"
            print(msg)
            _write_alert(alert, OUTPUT_PATH + "alerts/activity_alerts.json")
            alerts.append(alert)
    return alerts


def check_trending(df):
    game_titles = _load_game_titles()
    inverse_item_map = _load_item_map()
    alerts = []
    for row in df.collect():
        score = row.get("trending_score") if isinstance(row, dict) else getattr(row, "trending_score", None)
        item_id = row.get("item_id") if isinstance(row, dict) else getattr(row, "item_id", None)
        if score and score > TRENDING_THRESHOLD:
            asin = inverse_item_map.get(item_id, str(item_id))
            title = game_titles.get(asin, f"Game_{asin}")
            alert = {
                "alert_type": "trending",
                "item_id": item_id,
                "title": title,
                "trending_score": round(float(score), 4),
                "window_start": str(getattr(row, "window_start", "")),
                "window_end": str(getattr(row, "window_end", "")),
                "created_at": datetime.utcnow().isoformat()
            }
            msg = f"ALERT: Game {item_id} is trending — score {score:.2f}"
            print(msg)
            _write_alert(alert, OUTPUT_PATH + "alerts/trending_alerts.json")
            alerts.append(alert)
    return alerts
