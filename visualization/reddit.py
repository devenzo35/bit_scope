from storage.connection import get_db_connection
import streamlit as st
import pandas as pd

# Use a pipeline as a high-level helper
from transformers import pipeline

pipe = pipeline("text-classification", model="ProsusAI/finbert")


@st.cache_data
def proccess_in_batches(texts, batch_size=16):
    # sentiments = []
    all_results = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        batch_truncated = [text[:2000] for text in batch]
        results = pipe(batch_truncated, truncation=True, max_length=512)
        all_results.extend(results)

    return all_results


def reddit_page():
    con = get_db_connection()
    query = "SELECT * FROM reddit_posts"
    reddit_df = con.execute(query).df()

    reddit_df = reddit_df.head(15)

    reddit_df["selftext"] = reddit_df["selftext"].fillna("")
    reddit_df["combined_text"] = reddit_df["title"] + " " + reddit_df["selftext"]

    results = proccess_in_batches(texts=reddit_df["combined_text"].tolist())

    total_positive_score = []
    total_negative_score = []
    total_neutral_score = []

    for result in results:
        st.write(result)
        if result["label"] == "positive":
            total_positive_score.append(result["score"])
        if result["label"] == "negative":
            total_negative_score.append(result["score"])
        if result["label"] == "neutral":
            total_neutral_score.append(result["score"])

    avg_positive_score = sum(total_positive_score) / len(total_positive_score)
    avg_negative_score = sum(total_negative_score) / len(total_negative_score)
    avg_neutral_score = sum(total_neutral_score) / len(total_neutral_score)

    total_score = avg_positive_score - avg_negative_score

    st.metric(label="Average Positive Score", value=f"{avg_positive_score:4f}")
    st.metric(label="Average Negative Score", value=f"{avg_negative_score:4f}")
    st.metric(label="Average Neutral Score", value=f"{avg_neutral_score:4f}")

    st.metric(label="Total Positive Posts", value=f"{len(total_positive_score)}")
    st.metric(label="Total Negative Posts", value=f"{len(total_negative_score)}")
    st.metric(label="Total Neutral Posts", value=f"{len(total_neutral_score)}")

    if (
        avg_neutral_score > avg_positive_score
        and avg_neutral_score > avg_negative_score
    ):
        st.warning("Overall Sentiment: Neutral")
        st.metric(label="Total Sentiment Score", value=f"{avg_neutral_score:4f}")
    elif avg_negative_score > avg_positive_score:
        if avg_negative_score > 0.3 and avg_negative_score <= 0.5:
            st.warning("Warning:Very High Negative Sentiment Detected!")
        if avg_negative_score > 0.5 and avg_negative_score <= 0.7:
            st.error("Alert: High Negative Sentiment Detected!")
        if avg_negative_score > 0.7:
            st.error("Critical Alert: Extremely High Negative Sentiment Detected!")
        st.metric(label="Total Sentiment Score", value=f"{total_score:4f}")
    elif avg_positive_score > avg_negative_score:
        st.header("Overall Sentiment: Positive")
        st.metric(label="Total Sentiment Score", value=f"{total_score:4f}")


if __name__ == "__main__":
    reddit_page()
