import os
import time

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")

st.set_page_config(page_title="Real-Time Anomaly Engine", layout="wide")
st.title("Real-Time Streaming Analytics & Anomaly Engine")

with st.sidebar:
    st.header("Controls")
    lookback_min = st.slider("Lookback window (minutes)", 5, 120, 15)
    refresh_sec = st.slider("Auto-refresh interval (sec)", 2, 30, 5)
    selected_user = st.text_input("Filter by user_id (optional)")
    st.caption(f"API: {API_BASE_URL}")


@st.cache_data(ttl=2)
def fetch(path: str, params: dict):
    try:
        r = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        st.error(f"Request to {path} failed: {e}")
        return []


metrics = fetch("/metrics/recent", {"minutes": lookback_min, "user_id": selected_user or None})
anomalies = fetch("/anomalies/live", {"minutes": lookback_min})
stats = fetch("/anomalies/stats", {"minutes": lookback_min}) or {}

col1, col2, col3, col4 = st.columns(4)
col1.metric("Anomalies (window)", stats.get("total_anomalies") or 0)
col2.metric("Distinct users flagged", stats.get("distinct_users") or 0)
col3.metric("Avg |Z|", f"{(stats.get('avg_z_score') or 0):.2f}")
col4.metric("Max |Z|", f"{(stats.get('max_z_score') or 0):.2f}")

metrics_df = pd.DataFrame(metrics)
anomalies_df = pd.DataFrame(anomalies)

st.subheader("Rolling mean transaction amount per window")
if not metrics_df.empty:
    metrics_df["window_start"] = pd.to_datetime(metrics_df["window_start"])
    agg = (
        metrics_df.groupby("window_start", as_index=False)
        .agg(mean_amount=("mean_amount", "mean"), event_count=("event_count", "sum"))
        .sort_values("window_start")
    )
    fig = px.line(agg, x="window_start", y="mean_amount", title=None)

    if not anomalies_df.empty:
        anomalies_df["event_time"] = pd.to_datetime(anomalies_df["event_time"])
        fig.add_scatter(
            x=anomalies_df["event_time"],
            y=anomalies_df["amount"],
            mode="markers",
            marker=dict(color="red", size=10, symbol="x"),
            name="anomaly",
        )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Waiting for streaming data... start the producer and spark-processor services.")

st.subheader("Live anomaly feed")
if not anomalies_df.empty:
    st.dataframe(
        anomalies_df.sort_values("event_time", ascending=False)[
            ["event_time", "user_id", "amount", "z_score", "window_mean", "location", "device_id"]
        ],
        use_container_width=True,
        height=300,
    )
else:
    st.caption("No anomalies flagged in the selected window yet.")

st.subheader("Event volume per window (all users)")
if not metrics_df.empty:
    vol_fig = px.bar(agg, x="window_start", y="event_count")
    st.plotly_chart(vol_fig, use_container_width=True)

time.sleep(refresh_sec)
st.rerun()
