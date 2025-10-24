import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client, Client
from datetime import datetime
import pytz
import tzlocal


# -----------------------------
# 🔐 SUPABASE CONNECTION
# -----------------------------

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# ⚙️ DATA LOADER
# -----------------------------
@st.cache_data(ttl=1800)  # Cache data for 30 minutes
def load_data():
    """Load data from Supabase and convert UTC timestamps to local time."""
    response = supabase.table("reddit_sentiment").select("*").execute()
    df = pd.DataFrame(response.data)
    if df.empty:
        st.warning("No data found in the Supabase table yet.")
        return df

    # Convert UTC → Local time
    df["created_utc"] = pd.to_datetime(df["created_utc"], utc=True)
    local_tz = tzlocal.get_localzone()
    df["created_local"] = df["created_utc"].dt.tz_convert(local_tz)

    return df


# -----------------------------
# 🧭 DASHBOARD CONFIG
# -----------------------------
st.set_page_config(
    page_title="Reddit Sentiment Tracker",
    page_icon="📊",
    layout="wide"
)

st.title("📈 Reddit Sentiment Tracker — Topic Aware")
st.markdown(
    "Analyze sentiment trends across top subreddits — fetched in real-time from Supabase."
)


# -----------------------------
# 🔁 MANUAL REFRESH BUTTON
# -----------------------------
if st.button("🔄 Refresh Data Now"):
    st.cache_data.clear()
    st.experimental_rerun()


# -----------------------------
# 📥 LOAD DATA
# -----------------------------
df = load_data()
if df.empty:
    st.stop()

# Display timezone info
local_tz = tzlocal.get_localzone()
st.caption(f"🕒 Automatically displaying timestamps in your local timezone: {local_tz}")


# -----------------------------
# 🎯 SIDEBAR FILTERS
# -----------------------------
st.sidebar.header("🔍 Filter Options")

if "subreddit" in df.columns:
    subreddits = sorted(df["subreddit"].unique().tolist())
    selected_sub = st.sidebar.selectbox("Choose a subreddit", subreddits)
    filtered_df = df[df["subreddit"] == selected_sub]
else:
    st.warning("No subreddit column found in the data.")
    st.stop()

st.markdown(f"### Showing posts from **r/{selected_sub}** ({len(filtered_df)} posts)")


# -----------------------------
# 📊 SENTIMENT DISTRIBUTION
# -----------------------------
st.subheader("Sentiment Distribution")

sent_counts = (
    filtered_df["sentiment_label"]
    .value_counts()
    .reset_index()
    .rename(columns={"index": "sentiment_label", "sentiment_label": "count"})
)
sent_counts.columns = ["sentiment_label", "count"]

fig_sent = px.bar(
    sent_counts,
    x="sentiment_label",
    y="count",
    color="sentiment_label",
    title="Overall Sentiment Breakdown",
    labels={"sentiment_label": "Sentiment", "count": "Count"},
)
st.plotly_chart(fig_sent, use_container_width=True)


# -----------------------------
# 🕒 SENTIMENT SCORE OVER TIME
# -----------------------------
st.subheader("Sentiment Over Time")

filtered_df = filtered_df.sort_values("created_local")
filtered_df["rolling_mean"] = (
    filtered_df["sentiment_score"].rolling(window=5, min_periods=1).mean()
)

fig_time = px.scatter(
    filtered_df,
    x="created_local",
    y="sentiment_score",
    color="sentiment_label",
    title="Sentiment Score Timeline (Local Time)",
    hover_data=["title"],
)
fig_time.add_scatter(
    x=filtered_df["created_local"],
    y=filtered_df["rolling_mean"],
    mode="lines",
    name="Rolling Average",
    line=dict(color="black", width=2),
)
st.plotly_chart(fig_time, use_container_width=True)


# -----------------------------
# 🏆 TOP POSTS BY SCORE
# -----------------------------
st.subheader("Top Posts by Engagement")

top_posts = filtered_df.sort_values(by="score", ascending=False).head(10)
st.dataframe(
    top_posts[["title", "sentiment_label", "score", "created_local"]],
    use_container_width=True,
)
