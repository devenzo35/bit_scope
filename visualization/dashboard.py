import streamlit as st
from visualization.market_data_page import market_data_page
import plotly.graph_objects as go
from visualization.economics_page import economic_insights_page
from visualization.market_sentiment_page import market_sentiment_page

def main(page):
  
    if page == "Market Data Dashboard":
        market_data_page()
        
    if page == "Economic Insights":
        economic_insights_page()        
        
    if page == "Market Sentiment":
        market_sentiment_page()
      
if __name__ == "__main__":
    st.sidebar.title("Navigation")
    
    page = st.sidebar.selectbox("Go to", ["Market Data Dashboard", "Economic Insights", "Market Sentiment"])
       
    main(page)
     