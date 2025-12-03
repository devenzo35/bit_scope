import sys
from pathlib import Path

# Agregar el directorio padre al PYTHONPATH
root_dir = Path(__file__).parent.parent  # Sube dos niveles desde dashboard.py
sys.path.insert(0, str(root_dir))

import streamlit as st
from visualization.market_data_page import market_data_page
import plotly.graph_objects as go
from visualization.economics_page import economic_insights_page
from visualization.market_sentiment_page import market_sentiment_page
from visualization.reddit import reddit_page
from visualization.news import news_page


def main(page):
    if page == "News":
        news_page()

    if page == "Market Data Dashboard":
        market_data_page()

    if page == "Economic Insights":
        economic_insights_page()

    if page == "Market Sentiment":
        market_sentiment_page()

    if page == "Reddit Data Analysis":
        reddit_page()


if __name__ == "__main__":
    st.sidebar.title("Navigation")

    page = st.sidebar.selectbox(
        "Go to",
        [
            "News",
            "Market Data Dashboard",
            "Economic Insights",
            "Market Sentiment",
            "Reddit Data Analysis",
        ],
    )

    main(page)
