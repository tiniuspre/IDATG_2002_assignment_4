"""Streamlit entry point."""
from __future__ import annotations

import sqlite3

import streamlit as st

from app_pages import restaurants_page, visualization_page
from db_utils.db import get_connection, init_db


def configure() -> None:
    """Apply global Streamlit settings. Must run before any other st call."""
    st.set_page_config(page_title="Food Delivery Admin", layout="wide")


@st.cache_resource
def get_conn() -> sqlite3.Connection:
    """Return a shared SQLite connection for the Streamlit session."""
    return get_connection()


def main() -> None:
    """Configure Streamlit, init the DB, and dispatch to the selected page."""
    configure()
    init_db()
    conn = get_conn()

    pages = {
        "Restaurants": restaurants_page.render,
        "Visualization": visualization_page.render,
    }

    st.sidebar.title("Food Delivery Admin")
    choice = st.sidebar.radio("Page", list(pages.keys()))
    pages[choice](conn)


main()
