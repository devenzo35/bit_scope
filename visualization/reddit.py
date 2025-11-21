from storage.connection import get_db_connection
import streamlit as st
import pandas as pd


def reddit_page():
    con = get_db_connection()
    query = "SELECT * FROM reddit_posts"
    reddit_df = con.execute(query).df()

    st.title("Reddit Data Analysis")
    st.write(reddit_df)


if __name__ == "__main__":
    reddit_page()
