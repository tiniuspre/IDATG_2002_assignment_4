"""Visualization page."""
from __future__ import annotations

import sqlite3

import pandas as pd
import plotly.express as px
import streamlit as st

REVENUE_BY_CATEGORY_SQL = """
    SELECT c.name AS category,
           COUNT(o.order_id) AS order_count,
           ROUND(SUM(o.total_amount), 2) AS revenue
    FROM orders o
    JOIN restaurants r ON r.restaurant_id = o.restaurant_id
    LEFT JOIN categories c ON c.category_id = r.category_id
    WHERE o.status = 'delivered'
    GROUP BY c.name
    ORDER BY revenue DESC
"""


class VisualizationPage:
    """Renders the revenue-by-category chart."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Store the SQLite connection."""
        self.conn = conn

    def _fetch_data(self) -> pd.DataFrame:
        rows = self.conn.execute(REVENUE_BY_CATEGORY_SQL).fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["category"] = df["category"].fillna("(uncategorised)")
        return df

    def _render_chart(self, df: pd.DataFrame) -> None:
        fig = px.bar(
            df,
            x="category",
            y="revenue",
            text="revenue",
            labels={"category": "Category", "revenue": "Revenue"},
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    def _render_caption(self, df: pd.DataFrame) -> None:
        top = df.iloc[0]
        st.caption(
            f"Top category: {top['category']} "
            f"with {top['revenue']:.2f} in revenue "
            f"across {int(top['order_count'])} delivered orders."
        )

    def render(self) -> None:
        """Render the revenue-by-category page."""
        st.title("Revenue by category")
        st.write(
            "Total revenue from delivered orders, grouped by restaurant "
            "category. Joins orders, restaurants and categories."
        )

        df = self._fetch_data()
        if df.empty:
            st.info("No delivered orders yet, nothing to plot.")
            return

        self._render_chart(df)
        self._render_caption(df)

        st.subheader("Underlying data")
        st.dataframe(df, use_container_width=True, hide_index=True)


def render(conn: sqlite3.Connection) -> None:
    """Module-level entry called by the router in main.py."""
    VisualizationPage(conn).render()
