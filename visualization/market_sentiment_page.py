from storage.connection import get_db_connection
import streamlit as st
from plotly import graph_objects as go
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

plt.style.use("seaborn-v0_8-dark")
sns.set_palette("husl")


def market_sentiment_page():
    con = get_db_connection()
    df = con.execute("SELECT * FROM fear_and_greed").df()
    btc_price = con.execute("SELECT * FROM btc_prices").df()

    st.metric(label="Score", value=df["value"].iloc[0])
    st.metric(label="Classification", value=df["value_classification"].iloc[0])
    st.metric(label="Classification", value=str(df["date"].iloc[0]).split(" ")[0])

    df.set_index("date", inplace=True)

    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df_daily = df["value"].resample("15D").mean().reset_index()
    df_daily_btc = btc_price.set_index("date").resample("15D").mean().reset_index()

    df_daily["smooth_value"] = (
        df_daily["value"].rolling(window=15, min_periods=1).mean()
    )

    fig, ax1 = plt.subplots(figsize=(12, 6))

    line1 = ax1.plot(
        df_daily_btc["date"],
        df_daily_btc["price"],
        color="#00D9FF",
        linewidth=2,
        label="BTC Price",
    )

    ax1.set_xlabel("Date")
    ax1.set_ylabel("Sentiment Score", fontsize=15)
    ax1.tick_params(axis="y", labelsize=15)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax1.set_title("Bitcoin Market Sentiment vs Price")
    # how do I increment the font size of the y axis values?
    # ax1.tick_params(axis="y", labelsize=12)

    ax2 = ax1.twinx()

    line2 = ax2.plot(
        df_daily["date"],
        df_daily["value"],
        marker="o",
        color="#FF6B35",
        linewidth=2,
        label="Sentiment Score",
    )

    ax2.set_ylabel("BTC Price", fontsize=15)
    ax2.tick_params(axis="y", labelsize=15)
    ax2.set_ylim(0, 100)

    lines = line1 + line2
    labels = [line.get_label() for line in lines]

    ax1.legend(lines, labels, loc="upper left", fontsize=10)

    plt.title("Bitcoin Market Sentiment vs Price")
    plt.tight_layout()
    plt.xticks(rotation=45)

    st.pyplot(plt)
    plt.close()

    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=df_daily,
        x="date",
        y="value",
        marker="o",
        label="Sentiment Score",
        color="blue",
    )

    st.pyplot(plt)

    st.image("https://alternative.me/crypto/fear-and-greed-index.png")
