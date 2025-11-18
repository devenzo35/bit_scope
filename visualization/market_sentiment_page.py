from storage.connection import get_db_connection
import streamlit as st
from plotly import graph_objects as go
import pandas as pd


def market_sentiment_page():
    con = get_db_connection()
    df = con.execute("SELECT * FROM fear_and_greed").df()
    btc_price = con.execute("SELECT * FROM btc_prices").df()

    st.metric(label="Score", value=df["value"].iloc[0])
    st.metric(label="value_classification", value=df["classification"].iloc[0])

    df.set_index("date", inplace=True)

    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df_daily = df["value"].resample("D").mean().reset_index()
    df_daily_btc = btc_price.set_index("date").resample("D").mean(40).reset_index()

    df_daily["smooth_value"] = df_daily["value"].rolling(window=7, min_periods=1).mean()

    fig = go.Figure()

    fig.add_hrect(
        y0=0,
        y1=25,
        fillcolor="red",
        opacity=0.1,
        layer="below",
        line_width=0,
        yref="y2",  # Referencia al eje Y derecho
    )
    fig.add_hrect(
        y0=25,
        y1=45,
        fillcolor="orange",
        opacity=0.1,
        layer="below",
        line_width=0,
        yref="y2",
    )
    fig.add_hrect(
        y0=45,
        y1=55,
        fillcolor="yellow",
        opacity=0.1,
        layer="below",
        line_width=0,
        yref="y2",
    )
    fig.add_hrect(
        y0=55,
        y1=75,
        fillcolor="lightgreen",
        opacity=0.1,
        layer="below",
        line_width=0,
        yref="y2",
    )
    fig.add_hrect(
        y0=75,
        y1=100,
        fillcolor="green",
        opacity=0.1,
        layer="below",
        line_width=0,
        yref="y2",
    )

    fig.add_trace(
        go.Scatter(
            x=df_daily_btc["date"],
            y=df_daily_btc["price"],
            mode="lines",
            name="BTC Price",
            yaxis="y1",
            line=dict(color="white", width=1),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_daily["date"],
            y=df_daily["smooth_value"],
            mode="lines",
            name="Fear and Greed Index",
            yaxis="y2",
            line=dict(color="red", width=0.8, smoothing=1.3, shape="spline"),
        )
    )

    fig.update_layout(
        title="Bitcoin Fear and Greed Index Over Time",
        xaxis=dict(title="Date", domain=[1, 1]),
        yaxis=dict(title="BTC", side="left"),
        yaxis2=dict(title="Fear & Greed index", overlaying="y", side="right"),
        xaxis_title="Date",
        yaxis_title="Fear and Greed Index",
        hovermode="x unified",
        template="plotly_dark",
        height=600,
        width=1900,
    )

    st.plotly_chart(fig, use_container_width=True)
