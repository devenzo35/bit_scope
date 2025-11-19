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

    historic_avg = df_daily["value"].mean()

    fig = make_subplots(
        rows=1,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=("Sentiment vs Price"),
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

    fig2, ax = plt.subplots(figsize=(12, 6))
    ax.plot(
        df_daily["date"],
        df_daily["value"],
        linewidth=2,
        marker="o",
        label="Sentiment Score",
        color="blue",
    )
    ax.axhline(
        y=historic_avg,  # Altura de la línea (el promedio)
        color="red",  # Color dorado
        linestyle="--",  # Línea punteada
        linewidth=2,  # Grosor
        label=f"Promedio: ${historic_avg:,.0f}",  # Etiqueta con el valor
        alpha=0.8,  # Transparencia
        zorder=2,  # Que quede debajo del precio
    )

    fig3 = go.Figure()
    fig3.add_trace(
        go.Scatter(
            x=df_daily["date"],
            y=df_daily["value"],
            mode="lines+markers",
            name="Sentiment Score (Smoothed)",
            line=dict(
                color="white",  # ← Valores para el color
                width=1,
            ),
            marker=dict(
                color=df_daily["value"],  # También colorear los puntos
                colorscale="RdYlGn",
                size=10,
                showscale=True,  # No mostrar barra extra
                line=dict(width=1, color="white"),  # Borde blanco en markers
            ),
        )
    )

    fig3.add_trace(
        go.Scatter(
            x=df_daily["date"],
            y=[historic_avg] * len(df_daily),
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

    st.pyplot(plt)

    st.image("https://alternative.me/crypto/fear-and-greed-index.png")
