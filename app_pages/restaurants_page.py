"""Restaurants page."""
from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st

from db_utils.handlers import CategoryHandler, RestaurantHandler
from db_utils.utils import delete as delete_row

DISPLAY_COLUMNS = [
    "restaurant_id",
    "name",
    "category_name",
    "city",
    "street",
    "rating",
    "delivery_fee",
    "min_order_amt",
    "is_open",
]


class RestaurantsPage:
    """Renders the restaurants CRUD page."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Store the connection and build the handlers."""
        self.conn = conn
        self.restaurants = RestaurantHandler(conn)
        self.categories = CategoryHandler(conn)

    def _load_categories(self) -> list[dict]:
        return self.categories.list_all()

    def _category_options(self, cats: list[dict]) -> dict[str, int | None]:
        """Build a label-to-id mapping for selectboxes."""
        opts: dict[str, int | None] = {"(none)": None}
        for category in cats:
            opts[category["name"]] = category["category_id"]
        return opts

    def _load_rows(self) -> list[dict]:
        return self.restaurants.search(page=1, per_page=200)["items"]

    def _render_table(self, rows: list[dict]) -> None:
        st.subheader("All restaurants")
        if not rows:
            st.info("No restaurants in the database yet.")
            return
        df = pd.DataFrame(rows)
        cols = [c for c in DISPLAY_COLUMNS if c in df.columns]
        st.dataframe(df[cols], use_container_width=True, hide_index=True)

    def _render_create(self, cat_opts: dict[str, int | None]) -> None:
        st.subheader("Add a restaurant")
        with st.form("create_form", clear_on_submit=True):
            col_left, col_right = st.columns(2)

            with col_left:
                name = st.text_input("Name")
                street = st.text_input("Street")
                city = st.text_input("City")
                cat_label = st.selectbox("Category", list(cat_opts.keys()))

            with col_right:
                rating = st.number_input(
                    "Rating", min_value=0.0, max_value=5.0, value=0.0, step=0.1
                )
                delivery_fee = st.number_input(
                    "Delivery fee", min_value=0.0, value=0.0, step=1.0
                )
                min_order = st.number_input(
                    "Minimum order", min_value=0.0, value=0.0, step=1.0
                )
                is_open = st.checkbox("Open", value=True)

            if not st.form_submit_button("Add"):
                return

            if not name or not street or not city:
                st.error("Name, street and city are required.")
                return

            try:
                new_id = self.restaurants.create(
                    name=name,
                    street=street,
                    city=city,
                    category_id=cat_opts[cat_label],
                    rating=rating,
                    delivery_fee=delivery_fee,
                    min_order_amt=min_order,
                    is_open=int(is_open),
                )
                st.success(f"Added restaurant (id {new_id}).")
                st.rerun()
            except sqlite3.Error as exc:
                st.error(f"Could not add: {exc}")

    def _render_edit_delete(
        self,
        rows: list[dict],
        cat_opts: dict[str, int | None],
        cat_id_to_name: dict[int, str],
    ) -> None:
        st.subheader("Edit or delete")

        if not rows:
            st.caption("Nothing to edit yet.")
            return

        id_to_row = {row["restaurant_id"]: row for row in rows}
        labels = [
            f'{row["restaurant_id"]} - {row["name"]} ({row["city"]})'
            for row in rows
        ]
        choice = st.selectbox("Select restaurant", labels)
        sel_id = int(choice.split(" - ", 1)[0])
        sel = id_to_row[sel_id]

        with st.form("edit_form"):
            col_left, col_right = st.columns(2)

            with col_left:
                e_name = st.text_input("Name", value=sel["name"])
                e_street = st.text_input("Street", value=sel["street"])
                e_city = st.text_input("City", value=sel["city"])

                current_cat = cat_id_to_name.get(sel.get("category_id"), "(none)")
                cat_labels = list(cat_opts.keys())
                e_cat_label = st.selectbox(
                    "Category",
                    cat_labels,
                    index=(
                        cat_labels.index(current_cat)
                        if current_cat in cat_labels
                        else 0
                    ),
                )

            with col_right:
                e_rating = st.number_input(
                    "Rating",
                    min_value=0.0,
                    max_value=5.0,
                    value=float(sel["rating"]),
                    step=0.1,
                )
                e_fee = st.number_input(
                    "Delivery fee",
                    min_value=0.0,
                    value=float(sel["delivery_fee"]),
                    step=1.0,
                )
                e_min = st.number_input(
                    "Minimum order",
                    min_value=0.0,
                    value=float(sel["min_order_amt"]),
                    step=1.0,
                )
                e_open = st.checkbox("Open", value=bool(sel["is_open"]))

            col_save, col_delete = st.columns(2)
            save_clicked = col_save.form_submit_button("Save changes")
            delete_clicked = col_delete.form_submit_button("Delete")

        if save_clicked:
            self._handle_save(sel_id, {
                "name": e_name,
                "street": e_street,
                "city": e_city,
                "category_id": cat_opts[e_cat_label],
                "rating": e_rating,
                "delivery_fee": e_fee,
                "min_order_amt": e_min,
                "is_open": int(e_open),
            })

        if delete_clicked:
            self._handle_delete(sel_id)

    def _handle_save(self, sel_id: int, fields: dict) -> None:
        try:
            self.restaurants.update(sel_id, **fields)
            st.success("Saved.")
            st.rerun()
        except sqlite3.Error as exc:
            st.error(f"Could not save: {exc}")

    def _handle_delete(self, sel_id: int) -> None:
        try:
            delete_row(self.conn, "restaurants", "restaurant_id = ?", (sel_id,))
            st.success("Deleted.")
            st.rerun()
        except sqlite3.Error as exc:
            st.error(f"Could not delete: {exc}")

    def render(self) -> None:
        """Render the full restaurants page."""
        st.title("Restaurants")

        cats = self._load_categories()
        cat_opts = self._category_options(cats)
        cat_id_to_name = {c["category_id"]: c["name"] for c in cats}
        rows = self._load_rows()

        self._render_table(rows)
        st.divider()
        self._render_create(cat_opts)
        st.divider()
        self._render_edit_delete(rows, cat_opts, cat_id_to_name)


def render(conn: sqlite3.Connection) -> None:
    """Module-level entry called by the router in main.py."""
    RestaurantsPage(conn).render()
