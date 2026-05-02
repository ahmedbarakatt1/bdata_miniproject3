import sys
import os
import json
import glob
import pickle
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from config.config import OUTPUT_PATH, GAME_TITLES_PATH, TRENDING_THRESHOLD

st.set_page_config(layout="wide", page_title="Game Recommendation System")
st.title("Game Recommendation System — Live Dashboard")

try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=5000, key="autorefresh")
except ImportError:
    if st.button("Refresh"):
        st.rerun()


def load_game_titles():
    if os.path.exists(GAME_TITLES_PATH):
        with open(GAME_TITLES_PATH, "rb") as f:
            return pickle.load(f)
    return {}


def load_item_map():
    path = "data/raw/item_map.pkl"
    if os.path.exists(path):
        with open(path, "rb") as f:
            m = pickle.load(f)
        return {v: k for k, v in m.items()}
    return {}


def load_json_dir(path):
    rows = []
    for fn in glob.glob(path + "*.json"):
        try:
            with open(fn) as f:
                for line in f:
                    try:
                        rows.append(json.loads(line.strip()))
                    except:
                        pass
        except:
            pass
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def load_recommendations():
    path = OUTPUT_PATH + "recommendations/"
    files = glob.glob(path + "*.json")
    rows = []
    for fn in sorted(files)[-5:]:
        try:
            with open(fn) as f:
                for line in f:
                    try:
                        rows.append(json.loads(line.strip()))
                    except:
                        pass
        except:
            pass
    return pd.DataFrame(rows) if rows else pd.DataFrame()


game_titles = load_game_titles()
inverse_item_map = load_item_map()

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Top Recommendations")
    rec_df = load_recommendations()
    if not rec_df.empty and "user_id" in rec_df.columns:
        users = sorted(rec_df["user_id"].unique().tolist())
        selected_user = st.selectbox("Select User", users)
        user_recs = rec_df[rec_df["user_id"] == selected_user]
        if not user_recs.empty:
            st.dataframe(user_recs[["title", "score"]].head(5), use_container_width=True)
        else:
            st.info("No recommendations available for this user.")
    else:
        st.info("No recommendation data available yet.")

with col2:
    st.subheader("Trending Games")
    metrics_df = load_json_dir(OUTPUT_PATH + "metrics/items/")
    if not metrics_df.empty and "item_id" in metrics_df.columns and "trending_score" in metrics_df.columns:
        metrics_df["trending_score"] = pd.to_numeric(metrics_df["trending_score"], errors="coerce").fillna(0)
        top10 = metrics_df.nlargest(10, "trending_score")
        top10["title"] = top10["item_id"].apply(
            lambda iid: game_titles.get(inverse_item_map.get(iid, ""), f"Game_{iid}")
        )
        top10["color"] = top10["trending_score"].apply(lambda s: "red" if s > TRENDING_THRESHOLD else "steelblue")
        fig = px.bar(top10, x="title", y="trending_score",
                     color="color", color_discrete_map="identity",
                     labels={"title": "Game", "trending_score": "Trending Score"})
        fig.update_layout(showlegend=False, height=300)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No trending data available yet.")

with col3:
    st.subheader("User Activity")
    user_df = load_json_dir(OUTPUT_PATH + "metrics/users/")
    if not user_df.empty and "user_id" in user_df.columns and "interaction_count" in user_df.columns:
        user_df["interaction_count"] = pd.to_numeric(user_df["interaction_count"], errors="coerce").fillna(0)
        top_users = user_df.groupby("user_id")["interaction_count"].sum().nlargest(5).index.tolist()
        filt = user_df[user_df["user_id"].isin(top_users)]
        if "window_start" in filt.columns:
            fig2 = px.line(filt, x="window_start", y="interaction_count", color="user_id",
                           labels={"window_start": "Time", "interaction_count": "Interactions"})
            fig2.update_layout(height=300)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.dataframe(filt[["user_id", "interaction_count"]].head(10))
    else:
        st.info("No user activity data available yet.")

st.subheader("Live Alerts")
alert_files = glob.glob(OUTPUT_PATH + "alerts/*.json")
all_alerts = []
for fn in alert_files:
    try:
        with open(fn) as f:
            for line in f:
                try:
                    all_alerts.append(json.loads(line.strip()))
                except:
                    pass
    except:
        pass

if all_alerts:
    alerts_df = pd.DataFrame(all_alerts).tail(20)
    color_map = {"rating": "red", "activity": "orange", "trending": "yellow"}

    def color_row(row):
        color = color_map.get(row.get("alert_type", ""), "white")
        return [f"background-color: {color}; color: black"] * len(row)

    st.dataframe(alerts_df.style.apply(color_row, axis=1), use_container_width=True)
else:
    st.info("No alerts yet.")

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Avg Rating Over Time")
    if not metrics_df.empty and "avg_rating" in metrics_df.columns and "window_start" in metrics_df.columns:
        metrics_df["window_start"] = pd.to_datetime(metrics_df["window_start"], errors="coerce")
        recent = metrics_df.sort_values("window_start").tail(10 * 20)
        if not recent.empty:
            recent["title"] = recent["item_id"].apply(
                lambda iid: game_titles.get(inverse_item_map.get(iid, ""), f"Game_{iid}")
            )
            fig3 = px.line(recent, x="window_start", y="avg_rating", color="item_id",
                           labels={"window_start": "Window", "avg_rating": "Avg Rating"})
            fig3.update_layout(height=300)
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Not enough data for chart.")
    else:
        st.info("No rating data available yet.")

with col_right:
    st.subheader("Rating Variance Heatmap")
    if not metrics_df.empty and "rating_variance" in metrics_df.columns:
        metrics_df["rating_variance"] = pd.to_numeric(metrics_df["rating_variance"], errors="coerce").fillna(0)
        pivot_data = metrics_df.copy()
        pivot_data["window_label"] = pivot_data["window_start"].astype(str).str[:19]
        top_items = pivot_data.groupby("item_id")["interaction_count"].sum().nlargest(10).index
        pivot_filtered = pivot_data[pivot_data["item_id"].isin(top_items)]
        if not pivot_filtered.empty:
            try:
                pivot = pivot_filtered.pivot_table(
                    index="item_id", columns="window_label",
                    values="rating_variance", aggfunc="mean"
                )
                fig4 = px.imshow(pivot, labels=dict(x="Time Window", y="Item ID", color="Variance"),
                                 aspect="auto")
                fig4.update_layout(height=300)
                st.plotly_chart(fig4, use_container_width=True)
            except Exception as e:
                st.info(f"Heatmap unavailable: {e}")
        else:
            st.info("Not enough items for heatmap.")
    else:
        st.info("No variance data available yet.")
