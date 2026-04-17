from __future__ import annotations

import sqlite3
from typing import Any



def insert(
    conn: sqlite3.Connection,
    table: str,
    data: dict[str, Any],
) -> int:
    """INSERT a single row and return its rowid."""
    cols = ", ".join(data)
    placeholders = ", ".join(["?"] * len(data))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    cur = conn.execute(sql, list(data.values()))
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def bulk_insert(
    conn: sqlite3.Connection,
    table: str,
    rows: list[dict[str, Any]],
) -> int:
    """INSERT many rows in one call.  Returns the number of rows inserted."""
    if not rows:
        return 0
    cols = ", ".join(rows[0])
    placeholders = ", ".join(["?"] * len(rows[0]))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    cur = conn.executemany(sql, [list(r.values()) for r in rows])
    conn.commit()
    return cur.rowcount


def update(
    conn: sqlite3.Connection,
    table: str,
    data: dict[str, Any],
    where: str,
    params: tuple = (),
) -> int:
    """UPDATE rows matching `where`.  Returns affected row count."""
    set_clause = ", ".join(f"{k} = ?" for k in data)
    sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
    cur = conn.execute(sql, list(data.values()) + list(params))
    conn.commit()
    return cur.rowcount


def delete(
    conn: sqlite3.Connection,
    table: str,
    where: str,
    params: tuple = (),
) -> int:
    """DELETE rows matching `where`.  Returns affected row count."""
    sql = f"DELETE FROM {table} WHERE {where}"
    cur = conn.execute(sql, params)
    conn.commit()
    return cur.rowcount


def fetch_one(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple = (),
) -> dict | None:
    """Run a query and return the first row or None."""
    return conn.execute(sql, params).fetchone()


def fetch_all(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple = (),
) -> list[dict]:
    """Run a query and return every row."""
    return conn.execute(sql, params).fetchall()


def exists(
    conn: sqlite3.Connection,
    table: str,
    where: str,
    params: tuple = (),
) -> bool:
    """Return True if at least one matching row exists."""
    row = conn.execute(
        f"SELECT 1 FROM {table} WHERE {where} LIMIT 1", params
    ).fetchone()
    return row is not None


def count(
    conn: sqlite3.Connection,
    table: str,
    where: str = "1=1",
    params: tuple = (),
) -> int:
    """Return the number of rows matching `where`."""
    row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM {table} WHERE {where}", params
    ).fetchone()
    return row["cnt"]


def paginate(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple = (),
    *,
    page: int = 1,
    per_page: int = 20,
) -> dict:
    """
    Wrap a SELECT with LIMIT/OFFSET pagination.

    Returns {"items": [...], "page": int, "per_page": int, "total": int}.
    """
    count_sql = f"SELECT COUNT(*) AS cnt FROM ({sql})"
    total = conn.execute(count_sql, params).fetchone()["cnt"]

    offset = (max(page, 1) - 1) * per_page
    paged_sql = f"{sql} LIMIT ? OFFSET ?"
    items = conn.execute(paged_sql, (*params, per_page, offset)).fetchall()

    return {
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": total,
    }
