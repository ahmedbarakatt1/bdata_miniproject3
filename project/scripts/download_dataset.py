"""Download and preprocess the Amazon Video Games dataset."""
import os
import ast
import gzip
import json
import pickle
import requests
from tqdm import tqdm
import pandas as pd
from sklearn.preprocessing import LabelEncoder

RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

REVIEWS_URL = "https://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Video_Games.json.gz"
METADATA_URL = "https://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Video_Games.json.gz"


def download_file(url, dest):
    if os.path.exists(dest):
        print(f"Already exists: {dest}")
        return
    print(f"Downloading {url} ...")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True) as bar:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            bar.update(len(chunk))
    print(f"Saved to {dest}")


def parse_reviews():
    gz_path = os.path.join(RAW_DIR, "Video_Games.json.gz")
    download_file(REVIEWS_URL, gz_path)

    records = []
    print("Parsing reviews...")
    try:
        with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
            for line in tqdm(f):
                try:
                    obj = json.loads(line.strip())
                    records.append({
                        "reviewerID": obj.get("reviewerID"),
                        "asin": obj.get("asin"),
                        "overall": obj.get("overall"),
                        "unixReviewTime": obj.get("unixReviewTime")
                    })
                except:
                    continue
    except EOFError:
        print(f"Warning: gz file truncated. Proceeding with {len(records):,} records parsed so far.")

    df = pd.DataFrame(records).dropna()
    df = df.rename(columns={"overall": "rating", "unixReviewTime": "timestamp"})
    df["rating"] = df["rating"].astype(float)
    df["timestamp"] = df["timestamp"].astype(int)

    le_user = LabelEncoder()
    le_item = LabelEncoder()
    df["user_id"] = le_user.fit_transform(df["reviewerID"]) + 1
    df["item_id"] = le_item.fit_transform(df["asin"]) + 1

    user_map = {name: idx + 1 for idx, name in enumerate(le_user.classes_)}
    item_map = {name: idx + 1 for idx, name in enumerate(le_item.classes_)}

    with open(os.path.join(RAW_DIR, "user_map.pkl"), "wb") as f:
        pickle.dump(user_map, f)
    with open(os.path.join(RAW_DIR, "item_map.pkl"), "wb") as f:
        pickle.dump(item_map, f)

    out_df = df[["user_id", "item_id", "rating", "timestamp"]]
    out_df.to_parquet(os.path.join(RAW_DIR, "video_games_ratings.parquet"), index=False)

    print(f"\nTotal records: {len(df):,}")
    print(f"Unique users: {df['user_id'].nunique():,}")
    print(f"Unique items: {df['item_id'].nunique():,}")
    print(f"Rating distribution:\n{df['rating'].value_counts().sort_index()}")
    return df, item_map


def parse_metadata():
    gz_path = os.path.join(RAW_DIR, "meta_Video_Games.json.gz")
    download_file(METADATA_URL, gz_path)

    game_titles = {}
    print("Parsing metadata...")
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
        for line in tqdm(f):
            try:
                line = line.strip()
                try:
                    obj = json.loads(line)
                except Exception:
                    obj = ast.literal_eval(line)
                asin = obj.get("asin")
                title = obj.get("title") or obj.get("description") or f"Game_{asin}"
                if asin:
                    game_titles[asin] = str(title)[:100]
            except:
                continue

    with open(os.path.join(RAW_DIR, "game_titles.pkl"), "wb") as f:
        pickle.dump(game_titles, f)
    print(f"Saved {len(game_titles):,} game titles")
    return game_titles


if __name__ == "__main__":
    df, item_map = parse_reviews()
    game_titles = parse_metadata()
    print("\nDataset preparation complete.")
