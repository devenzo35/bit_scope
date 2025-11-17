import streamlit as st
from storage.connection import get_db_connection

import plotly.graph_objects as go


def show_dashboard():

    con = get_db_connection()

    st.title("Dashboard de Análisis de Datos Económicos")
    st.write("Visualizaciones y métricas clave se mostrarán aquí.")
    
    # Aquí se pueden agregar gráficos, tablas y otros elementos de visualización
    df = con.execute("SELECT * FROM btc_prices").df()
    
    df.head()
    df.tail()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['date'], y=df['price'], name='BTC Price'))
    fig.update_layout(title='Precio de Bitcoin a lo largo del tiempo', xaxis_title='Fecha', yaxis_title='Precio (USD)')
    st.plotly_chart(fig)
     
show_dashboard()