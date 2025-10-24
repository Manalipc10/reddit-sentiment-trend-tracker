from prefect import flow, task
import pandas as pd
import requests
from datetime import datetime
from textblob import TextBlob
from sqlalchemy import create_engine, text


# --- Replace this with your actual Supabase DB URI ---
DB_PATH = "postgresql://postgres:sahiladivarekar99@db.mcitrujsymkapoigslvs.supabase.co:5432/postgres"


# ------------------- TASKS -------------------

@task
def fetch_reddit_posts(subreddit="technology", limit=50):
    """Fetch latest Reddit posts."""
    try:
        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={limit}"
        headers = {"User-agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        data = res.json()["data"]["children"]
    except Exception as e:
        print("Reddit fetch error:", e)
        return pd.DataFrame(columns=["id", "title", "selftext", "created_utc", "subreddit", "score"])

    posts = []
    for d in data:
        post_data = d.get("data", {})
        posts.append({
            "id": post_data.get("id", ""),
            "title": post_data.get("title", "(no title)"),
            "selftext": post_data.get("selftext", ""),
            "created_utc": datetime.utcfromtimestamp(post_data.get("created_utc", 0)),
            "subreddit": post_data.get("subreddit", subreddit),
            "score": post_data.get("score", 0)
        })
    df = pd.DataFrame(posts)
    print(f"Fetched {len(df)} posts from r/{subreddit}")
    return df


@task
def analyze_sentiment(df):
    """Add sentiment scores using TextBlob."""
    if df.empty:
        return df
    df["sentiment_score"] = df["title"].astype(str).apply(lambda x: TextBlob(x).sentiment.polarity)
    df["sentiment_label"] = df["sentiment_score"].apply(
        lambda s: "positive" if s > 0 else "negative" if s < 0 else "neutral"
    )
    print("Sentiment analysis complete.")
    return df


@task
def store_to_db(df):
    """Store dataframe into Supabase PostgreSQL."""
    if df.empty:
        print("No data to store.")
        return
    try:
        engine = create_engine(DB_PATH)
        df.to_sql("reddit_sentiment", engine, if_exists="append", index=False)
        print(f"Stored {len(df)} rows in Supabase.")
    except Exception as e:
        print("Database storage error:", e)


# ------------------- FLOW -------------------

@flow(name="Reddit Sentiment Tracker - MultiTopic")
def reddit_sentiment_flow(limit=50):
    # LOOP STARTS HERE
    subreddits = ["technology", "worldnews", "science", "AskReddit", "todayilearned"]

    all_data = []

    for subreddit in subreddits:
        df = fetch_reddit_posts(subreddit=subreddit, limit=limit)
        if df.empty:
            continue
        df = analyze_sentiment(df)
        all_data.append(df)

    # COMBINE AND STORE
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        store_to_db(combined_df)
        print(f"✅ Processed {len(combined_df)} posts from {len(subreddits)} subreddits.")
    else:
        print("⚠️ No data fetched from any subreddit.")


# ------------------- MAIN -------------------

if __name__ == "__main__":
    reddit_sentiment_flow(limit=50)