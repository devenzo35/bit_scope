from storage.connection import get_db_connection
from plotly.tools import make_subplots
import streamlit as st
from plotly import graph_objects as go

def economic_insights_page():
    
    con = get_db_connection()
    df = con.execute("SELECT * FROM spy_price_data").df()
    st.write(df.head(10))
    
    return None

if __name__ == "__main__":
    economic_insights_page()