from storage.connection import get_db_connection
import streamlit as st
from plotly import graph_objects as go
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots

plt.style.use("seaborn-v0_8-dark")
sns.set_palette("husl")


def market_sentiment_page():
    con = get_db_connection()
    fng_df = con.execute("SELECT * FROM fear_and_greed").df()
    btc_price_df = con.execute("SELECT * FROM btc_prices").df()

    st.metric(label="Score", value=fng_df["value"].iloc[-1])
    st.metric(label="Classification", value=fng_df["value_classification"].iloc[-1])
    st.metric(label="Classification", value=str(fng_df["date"].iloc[-1]).split(" ")[0])

    fng_df.set_index("date", inplace=True)

    fng_df["value"] = pd.to_numeric(fng_df["value"], errors="coerce")

    fng_ma = fng_df["value"].resample("7D").mean().reset_index()
    btc_price_ma = btc_price_df.set_index("date").resample("7D").mean().reset_index()

    fng_ma["smooth_value"] = fng_ma["value"].rolling(window=15, min_periods=1).mean()

    historic_avg = fng_ma["value"].mean()

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=fng_ma["date"],
            y=fng_ma["value"],
            name="BTC Sentiment Score 7-Day MA",
            yaxis="y1",
            marker=dict(color=fng_ma["value"], colorscale="RdYlGn"),
            opacity=0.6,
        )
    )

    fig.add_trace(
        go.Scatter(
            x=btc_price_ma["date"],
            y=btc_price_ma["price"],
            mode="lines",
            name="BTC Price (USD) 7-Day MA",
            line=dict(color="white", width=3),
            yaxis="y2",
        )
    )

    fig.update_layout(
        height=600,
        xaxis=dict(title="Date", domain=[0.1, 0.9]),
        yaxis=dict(
            title="Sentiment Score",
            side="left",
        ),
        yaxis2=dict(
            title="BTC Price (USD)",
            overlaying="y",
            side="right",
        ),
        template="plotly_dark",
        hovermode="x unified",
        showlegend=True,
        title_text="Bitcoin Market Sentiment vs Price Over Time",
    )

    fig.update_legends(
        title_font=dict(size=14, color="white", family="Arial"),
        x=0.25,
        y=0.99,
    )
    st.plotly_chart(fig)

    fig3 = go.Figure()
    fig3.add_trace(
        go.Scatter(
            x=fng_ma["date"],
            y=fng_ma["value"],
            mode="lines+markers",
            name="Sentiment Score 7-Day ma",
            line=dict(
                color="white",  # ← Valores para el color
                width=1,
            ),
            marker=dict(
                color=fng_ma["value"],  # También colorear los puntos
                colorscale="RdYlGn",
                size=10,
                showscale=True,  # No mostrar barra extra
                line=dict(width=1, color="white"),  # Borde blanco en markers
            ),
        )
    )

    fig3.add_trace(
        go.Scatter(
            x=fng_ma["date"],
            y=[historic_avg] * len(fng_ma),
            mode="lines",
            name=f"Historic Average: {historic_avg:,.2f}",
            line=dict(color="red", width=2, dash="dash"),
        )
    )

    fig3.update_legends(
        title_font=dict(size=14, color="white", family="Arial"),
        x=0.68,
        y=0.99,
    )

    fig3.update_layout(
        title="Bitcoin Market Sentiment Over Time",
        xaxis_title="Date",
        yaxis_title="Sentiment Score",
        template="ggplot2",
        height=500,
        legend=dict(font=dict(size=12, color="white", family="Arial", weight="bold")),
    )

    st.plotly_chart(fig3)

    st.image("https://alternative.me/crypto/fear-and-greed-index.png")
