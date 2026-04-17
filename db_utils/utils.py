from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import sqlite3


_IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

ALLOWED_TABLES: frozenset[str] = frozenset(
    {
        'users',
        'addresses',
        'categories',
        'restaurants',
        'menu_items',
        'orders',
        'order_items',
        'drivers',
        'deliveries',
        'reviews',
    }
)

ALLOWED_COLUMNS: frozenset[str] = frozenset(
    {
        # users
        'user_id',
        'first_name',
        'last_name',
        'email',
        'phone',
        'created_at',
        # addresses
        'address_id',
        'label',
        'street',
        'city',
        'postal_code',
        'is_default',
        # categories
        'category_id',
        'name',
        'description',
        # restaurants
        'restaurant_id',
        'rating',
        'delivery_fee',
        'min_order_amt',
        'is_open',
        # menu_items
        'item_id',
        'price',
        'is_available',
        'is_vegetarian',
        'is_vegan',
        # orders
        'order_id',
        'status',
        'subtotal',
        'total_amount',
        'payment_method',
        'ordered_at',
        # order_items
        'order_item_id',
        'quantity',
        'unit_price',
        # drivers
        'driver_id',
        'vehicle_type',
        # deliveries
        'delivery_id',
        'picked_up_at',
        'delivered_at',
        'estimated_mins',
        'distance_km',
        # reviews
        'review_id',
        'food_rating',
        'delivery_rating',
        'comment',
    }
)


def _validate_identifier(value: str, kind: str) -> str:
    """Raise ``ValueError`` if *value* is not a safe SQL identifier."""
    if not _IDENT_RE.match(value):
        msg = f'Invalid {kind}: {value!r}'
        raise ValueError(msg)
    return value


def _validate_table(name: str) -> str:
    """Raise ``ValueError`` if *name* is not in the table allowlist."""
    _validate_identifier(name, 'table name')
    if name not in ALLOWED_TABLES:
        msg = f'Table not in allowlist: {name!r}'
        raise ValueError(msg)
    return name


def _validate_column(name: str) -> str:
    """Raise ``ValueError`` if *name* is not in the column allowlist."""
    _validate_identifier(name, 'column name')
    if name not in ALLOWED_COLUMNS:
        msg = f'Column not in allowlist: {name!r}'
        raise ValueError(msg)
    return name


def _validate_columns(names: list[str] | tuple[str, ...]) -> None:
    """Validate a batch of column names."""
    for n in names:
        _validate_column(n)


# ------------------------------------------------------------------
# Generic helpers
# ------------------------------------------------------------------


def insert(
    conn: sqlite3.Connection,
    table: str,
    data: dict[str, Any],
) -> int:
    """INSERT a single row and return its rowid."""
    _validate_table(table)
    cols = list(data.keys())
    _validate_columns(cols)
    col_str = ', '.join(cols)
    placeholders = ', '.join(['?'] * len(cols))
    sql = f'INSERT INTO {table} ({col_str}) VALUES ({placeholders})'  # noqa: S608
    cur = conn.execute(sql, list(data.values()))
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def bulk_insert(
    conn: sqlite3.Connection,
    table: str,
    rows: list[dict[str, Any]],
) -> int:
    """INSERT many rows in one call.  Returns rows inserted."""
    if not rows:
        return 0
    _validate_table(table)
    cols = list(rows[0].keys())
    _validate_columns(cols)
    col_str = ', '.join(cols)
    placeholders = ', '.join(['?'] * len(cols))
    sql = f'INSERT INTO {table} ({col_str}) VALUES ({placeholders})'  # noqa: S608
    cur = conn.executemany(
        sql,
        [list(r.values()) for r in rows],
    )
    conn.commit()
    return cur.rowcount


def update(
    conn: sqlite3.Connection,
    table: str,
    data: dict[str, Any],
    where: str,
    params: tuple = (),
) -> int:
    """UPDATE rows matching *where*.  Returns affected row count."""
    _validate_table(table)
    cols = list(data.keys())
    _validate_columns(cols)
    set_clause = ', '.join(f'{c} = ?' for c in cols)
    sql = f'UPDATE {table} SET {set_clause} WHERE {where}'  # noqa: S608
    cur = conn.execute(sql, list(data.values()) + list(params))
    conn.commit()
    return cur.rowcount


def delete(
    conn: sqlite3.Connection,
    table: str,
    where: str,
    params: tuple = (),
) -> int:
    """DELETE rows matching *where*.  Returns affected row count."""
    _validate_table(table)
    sql = f'DELETE FROM {table} WHERE {where}'  # noqa: S608
    cur = conn.execute(sql, params)
    conn.commit()
    return cur.rowcount


def fetch_one(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple = (),
) -> dict | None:
    """Run a query and return the first row or ``None``."""
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
    """Return ``True`` if at least one matching row exists."""
    _validate_table(table)
    row = conn.execute(
        f'SELECT 1 FROM {table} WHERE {where} LIMIT 1',  # noqa: S608
        params,
    ).fetchone()
    return row is not None


def count(
    conn: sqlite3.Connection,
    table: str,
    where: str = '1=1',
    params: tuple = (),
) -> int:
    """Return the number of rows matching *where*."""
    _validate_table(table)
    row = conn.execute(
        f'SELECT COUNT(*) AS cnt FROM {table} WHERE {where}',  # noqa: S608
        params,
    ).fetchone()
    return row['cnt']


# ------------------------------------------------------------------
# Pagination helper
# ------------------------------------------------------------------


def paginate(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple = (),
    *,
    page: int = 1,
    per_page: int = 20,
) -> dict:
    """Wrap a SELECT with LIMIT/OFFSET pagination.

    Returns ``{"items": [...], "page", "per_page", "total"}``.
    """
    count_sql = f'SELECT COUNT(*) AS cnt FROM ({sql})'  # noqa: S608
    total = conn.execute(count_sql, params).fetchone()['cnt']

    offset = (max(page, 1) - 1) * per_page
    paged_sql = f'{sql} LIMIT ? OFFSET ?'
    items = conn.execute(
        paged_sql,
        (*params, per_page, offset),
    ).fetchall()

    return {
        'items': items,
        'page': page,
        'per_page': per_page,
        'total': total,
    }
