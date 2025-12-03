import streamlit as st
from storage.connection import get_db_connection


def news_page():
    con = get_db_connection()
    df = con.execute("SELECT * FROM coindesk_articles").df()

    st.title("Coindesk Opinion Page Articles")

    for index, row in df.iterrows():
        st.subheader(row["title"])
        st.write(f"{row['extra_info']}")
        st.write("---")


if __name__ == "__main__":
    news_page()
