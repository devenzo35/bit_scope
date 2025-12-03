from storage.connection import get_db_connection
import streamlit as st
import pandas as pd
from plotly import express as px

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

    # reddit_df = reddit_df.head(15)

    reddit_df["selftext"] = reddit_df["selftext"].fillna("")
    reddit_df["combined_text"] = reddit_df["title"] + " " + reddit_df["selftext"]

    results = proccess_in_batches(texts=reddit_df["combined_text"].tolist())

    total_positive_score = []
    total_negative_score = []
    total_neutral_score = []

    for result in results:
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

    st.title("Reddit Posts Sentiment Analysis")

    if (
        avg_neutral_score > avg_positive_score
        and avg_neutral_score > avg_negative_score
    ):
        st.metric(label="Neutral Sentiment Score", value=f"{avg_neutral_score:4f}")
        st.warning("Overall Sentiment: Neutral")
    elif avg_negative_score > avg_positive_score:
        st.metric(label="Total Sentiment Score", value=f"{total_score:4f}")
        if avg_negative_score > 0.3 and avg_negative_score <= 0.5:
            st.error("Negative Sentiment Detected!")
        if avg_negative_score > 0.5 and avg_negative_score <= 0.7:
            st.error("High Negative Sentiment Detected!")
        if avg_negative_score > 0.7:
            st.error(" Extremely High Negative Sentiment Detected!")
    elif avg_positive_score > avg_negative_score:
        st.metric(label="Total Sentiment Score", value=f"{total_score:4f}")
        if avg_positive_score > 0.3 and avg_positive_score <= 0.5:
            st.success("Positive Sentiment Detected!")
        if avg_positive_score > 0.5 and avg_positive_score <= 0.7:
            st.success("Very High Positive Sentiment Detected!")
        if avg_positive_score > 0.7:
            st.success("Extremely High Positive Sentiment Detected!")

    st.header("Average Sentiment")

    avg_col1, avg_col2, avg_col3 = st.columns(3)

    with avg_col1:
        st.metric(label="Average Positive Score", value=f"{avg_positive_score:1f}")
    with avg_col2:
        st.metric(label="Average Negative Score", value=f"{avg_negative_score:2f}")
    with avg_col3:
        st.metric(label="Average Neutral Score", value=f"{avg_neutral_score:2f}")

    fig = px.bar(
        x=["Positive", "Negative", "Neutral"],
        y=[
            avg_positive_score,
            avg_negative_score,
            avg_neutral_score,
        ],
        labels={"x": "", "y": "Average Sentiment Score"},
        color=["Positive", "Negative", "Neutral"],
        color_discrete_map={
            "Positive": "#7DCFB6",
            "Negative": "#E63946",
            "Neutral": "#D4B483",
        },
    )

    st.plotly_chart(fig)

    st.header("Total Posts divided by Sentiment")

    total_col1, total_col2, total_col3 = st.columns(3)

    with total_col1:
        st.metric(label="Total Positive Posts", value=f"{len(total_positive_score)}")
    with total_col2:
        st.metric(label="Total Negative Posts", value=f"{len(total_negative_score)}")
    with total_col3:
        st.metric(label="Total Neutral Posts", value=f"{len(total_neutral_score)}")

    fig2 = px.bar(
        y=[
            len(total_positive_score),
            len(total_negative_score),
            len(total_neutral_score),
        ],
        x=["Positive", "Negative", "Neutral"],
        color=["Positive", "Negative", "Neutral"],
        color_discrete_map={
            "Positive": "#7DCFB6",
            "Negative": "#E63946",
            "Neutral": "#D4B483",
        },
        labels={"x": "", "y": "Total Posts"},
    )

    st.plotly_chart(fig2)


if __name__ == "__main__":
    reddit_page()
