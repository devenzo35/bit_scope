from storage.connection import get_db_connection
from plotly.tools import make_subplots
import streamlit as st
from plotly import graph_objects as go
import datetime as dt


def economic_insights_page():
    con = get_db_connection()
    spy_df = con.execute("SELECT * FROM spy_price_data").df()
    interest_rates_df = con.execute("SELECT * FROM interest_rates_data").df()
    unemp_rate_df = con.execute("SELECT * FROM unemployment_rate_data").df()
    cpi_df = con.execute("SELECT * FROM cpi_data").df()

    date_range = st.select_slider(
        "Select a range of dates",
        options=spy_df["date"].sort_values().unique(),
        value=(spy_df["date"].min(), spy_df["date"].max()),
    )

    spy_df = spy_df[
        (spy_df["date"] >= date_range[0]) & (spy_df["date"] <= date_range[1])
    ]

    daily_interest_rate_df = (
        interest_rates_df.set_index("date").resample("D").ffill().reset_index()
    )
    daily_spy_df = spy_df.set_index("date").resample("H").ffill().reset_index()
    daily_unemp_rate_df = (
        unemp_rate_df.set_index("date").resample("M").ffill().reset_index()
    )
    daily_cpi_df = cpi_df.set_index("date").resample("M").ffill().reset_index()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=daily_spy_df["date"],
            y=daily_spy_df["price"],
            mode="lines",
            name="SPY Close Price",
            line=dict(color="#1f77b4"),
        )
    )

    fig.update_layout(
        title="SPY Daily Close Price",
        xaxis_title="Date",
        yaxis_title="Price (USD)",
    )

    st.plotly_chart(fig)

    fig2 = go.Figure()

    fig2.add_trace(
        go.Scatter(
            x=daily_interest_rate_df["date"],
            y=daily_interest_rate_df["interest_rate"],
            mode="lines",
            name="Interest Rate (%)",
            hovertemplate="<b>Rate:</b> %{y}%<extra></extra>",
            line=dict(color="#6E8CFB"),
        )
    )

    fig2.update_layout(
        title="Interest Rate",
        yaxis_title="Interest Rate (%)",
        xaxis_title="Date",
        hovermode="x unified",
    )

    st.plotly_chart(fig2)

    unemp_rate_range = st.select_slider(
        label="Select temporal range for unemployment rate data",
        options=["1Y", "5Y", "10Y", "All"],
        key="unemp_rate_date_range",
        value="All",
    )

    if "filtered_unemp_rate" not in st.session_state:
        st.session_state["filtered_unemp_rate"] = daily_unemp_rate_df
    if unemp_rate_range == "1Y":
        year_ago = dt.datetime.today() - dt.timedelta(days=365)
        st.session_state.filtered_unemp_rate = daily_unemp_rate_df[
            daily_unemp_rate_df["date"] >= year_ago
        ]
    if unemp_rate_range == "5Y":
        year_ago = dt.datetime.today() - dt.timedelta(days=365 * 5)
        st.session_state.filtered_unemp_rate = daily_unemp_rate_df[
            daily_unemp_rate_df["date"] >= year_ago
        ]
    if unemp_rate_range == "10Y":
        year_ago = dt.datetime.today() - dt.timedelta(days=365 * 10)
        st.session_state.filtered_unemp_rate = daily_unemp_rate_df[
            daily_unemp_rate_df["date"] >= year_ago
        ]
    if unemp_rate_range == "All":
        st.session_state.filtered_unemp_rate = daily_unemp_rate_df
    filtered_unemp_rate = st.session_state.filtered_unemp_rate

    fig3 = go.Figure()
    fig3.add_trace(
        go.Bar(
            x=filtered_unemp_rate["date"],
            y=filtered_unemp_rate["unemp_rate"],
            name="Unemployment Rate (%)",
            hovertemplate="<b>Rate:</b> %{y}%<extra></extra>",
            marker=dict(color="#1f77b4"),
        )
    )

    unemp_avg = daily_unemp_rate_df["unemp_rate"].mean()

    fig3.add_trace(
        go.Scatter(
            x=filtered_unemp_rate["date"],
            y=[unemp_avg] * len(filtered_unemp_rate),
            mode="lines",
            name="Average Unemployment Rate",
            line=dict(color="red", dash="dash"),
        )
    )

    fig3.update_layout(
        title="Unemployment Rate",
        yaxis_title="Unemployment Rate (%)",
        xaxis_title="Date",
        hovermode="x unified",
    )

    ymin = filtered_unemp_rate["unemp_rate"].min()
    ymax = filtered_unemp_rate["unemp_rate"].max()

    padding = (ymax - ymin) * 0.2  # agrega 20% de espacio

    fig3.update_yaxes(range=[ymin - padding, ymax + padding])
    st.plotly_chart(fig3)

    cpi_data_range = st.select_slider(
        label="Select temporal range for CPI data",
        options=["1Y", "5Y", "10Y", "All"],
        key="cpi_date_range",
        value="All",
    )

    if "filtered_cpi" not in st.session_state:
        st.session_state["filtered_cpi"] = daily_cpi_df
    if cpi_data_range == "1Y":
        year_ago = dt.datetime.today() - dt.timedelta(days=365)
        st.session_state.filtered_cpi = daily_cpi_df[daily_cpi_df["date"] >= year_ago]
    if cpi_data_range == "5Y":
        year_ago = dt.datetime.today() - dt.timedelta(days=365 * 5)
        st.session_state.filtered_cpi = daily_cpi_df[daily_cpi_df["date"] >= year_ago]
    if cpi_data_range == "10Y":
        year_ago = dt.datetime.today() - dt.timedelta(days=365 * 10)
        st.session_state.filtered_cpi = daily_cpi_df[daily_cpi_df["date"] >= year_ago]
    if cpi_data_range == "All":
        st.session_state.filtered_cpi = daily_cpi_df
    filtered_cpi = st.session_state.filtered_cpi

    fig4 = go.Figure()
    fig4.add_trace(
        go.Bar(
            x=filtered_cpi["date"],
            y=filtered_cpi["monthly_inflation"],
            name="Consumer Price Index (CPI)",
            hovertemplate="<b>CPI:</b> %{y}<extra></extra>",
            marker=dict(color="#ff7f0e"),
        )
    )
    cpi_avg = daily_cpi_df["monthly_inflation"].mean()

    fig4.add_trace(
        go.Scatter(
            x=filtered_cpi["date"],
            y=[cpi_avg] * len(filtered_cpi),
            mode="lines",
            name="Average Consumer Price Index",
            line=dict(color="red", dash="dash"),
        )
    )
    fig4.update_layout(
        title="Consumer Price Index (CPI)",
        hovermode="x unified",
    )
    st.plotly_chart(fig4)


if __name__ == "__main__":
    economic_insights_page()
