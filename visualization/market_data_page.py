from storage.connection import get_db_connection
from plotly.tools import make_subplots
import streamlit as st
from plotly import graph_objects as go

def market_data_page():
    """
    Placeholder function for market data dashboard visualization.
    This function is intended to be implemented with specific visualization logic.
    """
    
    con = get_db_connection()
    df = con.execute("SELECT * FROM btc_prices").df()

    st.title("BTC Market Data Dashboard")
                
    date_range = st.select_slider("Select a range of dates", options=df['date'].sort_values().unique(), value=(df['date'].min(), df['date'].max()))
        
    filtered_df = df[(df['date'] >= date_range[0]) & (df['date'] <= date_range[1]) ]
            
    filtered_df.set_index('date', inplace=True)
    df_daily = filtered_df.resample('D').mean().reset_index()

        
    fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            subplot_titles=("Price", "Volume"),
            row_heights=[0.7, 0.3]
        )
        
    st.subheader("Basic statistics")
    col1, col2, col3, col4 = st.columns(spec=4, gap="small")
    ath = df['price'].max()
    last_date = df[df['date'] >= df['date'].max()]
    last_price = last_date['price'].iloc[-1]
    previous_price = df['price'].iloc[-1]
    
    with col1:
        st.metric(label='Price', value=f'${last_price:,.2f}')
    with col2:
        pct_change = ((last_price - previous_price) / previous_price) * 100      
        st.metric(label="24h Change", value=f'{last_price - previous_price}', delta=f'{pct_change:.2f}%', width='content')
    with col3:
        st.metric(label="ATH" ,value=f'${ath:,.1f}', width='content')
    with col4:
        distance_from_ath = (1 - (last_price / ath)) * 100
        st.metric(label="Distance from ATH", value=f'${ath - last_price:,.2f}', delta=f'{-distance_from_ath:.2f}%', width='stretch', delta_color="off")
    YTD = df_daily[df_daily['date'] >= f"{df_daily['date'].max().year}-01-01"]
    ytd_return = ((YTD['price'].iloc[-1] - YTD['price'].iloc[0]) / YTD['price'].iloc[0]) * 100
    st.metric(label="YTD Return", value=f'${YTD["price"].iloc[-1] - YTD["price"].iloc[0]:,.2f}', delta=f'{ytd_return:.2f}%')
    
        
        
        
    fig.add_trace(go.Scatter(x=df_daily['date'], y=df_daily['price'], mode='lines', name='Price'), row=1, col=1)
    fig.add_trace(go.Bar(x=df_daily['date'], y=df_daily['total_volume'],  marker_color='#FF6B35',name='Volume'), row=2, col=1)
    
    fig.update_layout(height=600, template= 'plotly_dark', hovermode="x unified" ,showlegend=True ,title_text="Bitcoin Price and Market Cap Over Time")
    
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    
    st.plotly_chart(fig)
        
if __name__ == "__main__":
    marketdata_dshb()