from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

DB_PATH = os.getenv(
    "FOOD_DELIVERY_DB", str(Path(__file__).with_name("food_delivery.db"))
)

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    user_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name     TEXT    NOT NULL,
    last_name      TEXT    NOT NULL,
    email          TEXT    NOT NULL UNIQUE,
    phone          TEXT    NOT NULL,
    created_at     TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS addresses (
    address_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id        INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    label          TEXT    NOT NULL DEFAULT 'Home',
    street         TEXT    NOT NULL,
    city           TEXT    NOT NULL,
    postal_code    TEXT    NOT NULL,
    is_default     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_addresses_user ON addresses(user_id);

CREATE TABLE IF NOT EXISTS categories (
    category_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT    NOT NULL UNIQUE,
    description    TEXT
);

CREATE TABLE IF NOT EXISTS restaurants (
    restaurant_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id    INTEGER REFERENCES categories(category_id) ON DELETE SET NULL,
    name           TEXT    NOT NULL,
    street         TEXT    NOT NULL,
    city           TEXT    NOT NULL,
    rating         REAL    NOT NULL DEFAULT 0.0,
    delivery_fee   REAL    NOT NULL DEFAULT 0.0,
    min_order_amt  REAL    NOT NULL DEFAULT 0.0,
    is_open        INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_restaurants_category ON restaurants(category_id);
CREATE INDEX IF NOT EXISTS idx_restaurants_city     ON restaurants(city);

CREATE TABLE IF NOT EXISTS menu_items (
    item_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    restaurant_id  INTEGER NOT NULL REFERENCES restaurants(restaurant_id) ON DELETE CASCADE,
    name           TEXT    NOT NULL,
    price          REAL    NOT NULL CHECK (price >= 0),
    is_available   INTEGER NOT NULL DEFAULT 1,
    is_vegetarian  INTEGER NOT NULL DEFAULT 0,
    is_vegan       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_menu_items_restaurant ON menu_items(restaurant_id);

CREATE TABLE IF NOT EXISTS orders (
    order_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id        INTEGER NOT NULL REFERENCES users(user_id),
    restaurant_id  INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    address_id     INTEGER NOT NULL REFERENCES addresses(address_id),
    status         TEXT    NOT NULL DEFAULT 'pending'
                          CHECK (status IN (
                              'pending','confirmed','preparing',
                              'ready','picked_up','delivered','cancelled'
                          )),
    subtotal       REAL    NOT NULL DEFAULT 0.0,
    total_amount   REAL    NOT NULL DEFAULT 0.0,
    payment_method TEXT    NOT NULL DEFAULT 'card'
                          CHECK (payment_method IN ('card','cash','wallet')),
    ordered_at     TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_orders_user       ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_restaurant ON orders(restaurant_id);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id       INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    item_id        INTEGER NOT NULL REFERENCES menu_items(item_id),
    quantity       INTEGER NOT NULL CHECK (quantity > 0),
    unit_price     REAL    NOT NULL CHECK (unit_price >= 0)
);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name     TEXT    NOT NULL,
    last_name      TEXT    NOT NULL,
    phone          TEXT    NOT NULL,
    vehicle_type   TEXT    NOT NULL DEFAULT 'bicycle'
                          CHECK (vehicle_type IN ('bicycle','scooter','car')),
    rating         REAL    NOT NULL DEFAULT 5.0
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id       INTEGER NOT NULL UNIQUE REFERENCES orders(order_id),
    driver_id      INTEGER NOT NULL REFERENCES drivers(driver_id),
    picked_up_at   TEXT,
    delivered_at   TEXT,
    estimated_mins INTEGER,
    distance_km    REAL
);
CREATE INDEX IF NOT EXISTS idx_deliveries_order  ON deliveries(order_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_driver ON deliveries(driver_id);

CREATE TABLE IF NOT EXISTS reviews (
    review_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id       INTEGER NOT NULL UNIQUE REFERENCES orders(order_id),
    user_id        INTEGER NOT NULL REFERENCES users(user_id),
    restaurant_id  INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    food_rating    INTEGER NOT NULL CHECK (food_rating BETWEEN 1 AND 5),
    delivery_rating INTEGER NOT NULL CHECK (delivery_rating BETWEEN 1 AND 5),
    comment        TEXT
);
CREATE INDEX IF NOT EXISTS idx_reviews_user       ON reviews(user_id);
CREATE INDEX IF NOT EXISTS idx_reviews_restaurant ON reviews(restaurant_id);
"""


def _row_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    """Return rows as dicts instead of tuples."""
    return {col[0]: row[i] for i, col in enumerate(cursor.description)}


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Open a connection with sensible defaults."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = _row_factory
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def get_db(db_path: str = DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    """Context manager that commits on success, rolls back on error."""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str = DB_PATH) -> None:
    """Create all tables and indices (idempotent)."""
    with get_db(db_path) as conn:
        conn.executescript(SCHEMA_SQL)


if __name__ == "__main__":
    init_db()
    print(f"Database initialised at {DB_PATH}")
