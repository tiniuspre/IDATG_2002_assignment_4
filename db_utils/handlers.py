"""Domain-specific database handlers for the food-delivery platform.

Each class groups the queries for one logical area and receives an
open ``sqlite3.Connection`` on construction so callers can share
transactions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from db_utils.utils import (
    bulk_insert,
    count,
    delete,
    exists,
    fetch_all,
    fetch_one,
    insert,
    paginate,
    update,
)

if TYPE_CHECKING:
    import sqlite3


# ##
# Users & Addresses
# ##


class UserHandler:
    """CRUD operations for the ``users`` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind the handler to *conn*."""
        self.conn = conn

    def create(
        self,
        *,
        first_name: str,
        last_name: str,
        email: str,
        phone: str,
    ) -> int:
        """Insert a new user and return its id."""
        return insert(
            self.conn,
            'users',
            {
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': phone,
            },
        )

    def get(self, user_id: int) -> dict | None:
        """Return a single user by primary key."""
        return fetch_one(
            self.conn,
            'SELECT * FROM users WHERE user_id = ?',
            (user_id,),
        )

    def get_by_email(self, email: str) -> dict | None:
        """Look up a user by email address."""
        return fetch_one(
            self.conn,
            'SELECT * FROM users WHERE email = ?',
            (email,),
        )

    def update(
        self,
        user_id: int,
        **fields: str | int,
    ) -> int:
        """Update one or more columns on a user row."""
        return update(
            self.conn,
            'users',
            fields,
            'user_id = ?',
            (user_id,),
        )

    def delete(self, user_id: int) -> int:
        """Delete a user by primary key."""
        return delete(
            self.conn,
            'users',
            'user_id = ?',
            (user_id,),
        )

    def list_all(
        self,
        *,
        page: int = 1,
        per_page: int = 20,
    ) -> dict:
        """Return a paginated list of all users."""
        return paginate(
            self.conn,
            'SELECT * FROM users ORDER BY created_at DESC',
            page=page,
            per_page=per_page,
        )


class AddressHandler:
    """CRUD operations for the ``addresses`` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind the handler to *conn*."""
        self.conn = conn

    def create(
        self,
        *,
        user_id: int,
        label: str,
        street: str,
        city: str,
        postal_code: str,
        is_default: int = 0,
    ) -> int:
        """Insert a new address for *user_id*."""
        if is_default:
            self._clear_defaults(user_id)
        return insert(
            self.conn,
            'addresses',
            {
                'user_id': user_id,
                'label': label,
                'street': street,
                'city': city,
                'postal_code': postal_code,
                'is_default': is_default,
            },
        )

    def get(self, address_id: int) -> dict | None:
        """Return a single address by primary key."""
        return fetch_one(
            self.conn,
            'SELECT * FROM addresses WHERE address_id = ?',
            (address_id,),
        )

    def list_for_user(self, user_id: int) -> list[dict]:
        """Return all addresses for *user_id*."""
        return fetch_all(
            self.conn,
            'SELECT * FROM addresses WHERE user_id = ? ORDER BY is_default DESC',
            (user_id,),
        )

    def set_default(
        self,
        user_id: int,
        address_id: int,
    ) -> None:
        """Mark *address_id* as the default for *user_id*."""
        self._clear_defaults(user_id)
        update(
            self.conn,
            'addresses',
            {'is_default': 1},
            'address_id = ?',
            (address_id,),
        )

    def delete(self, address_id: int) -> int:
        """Delete an address by primary key."""
        return delete(
            self.conn,
            'addresses',
            'address_id = ?',
            (address_id,),
        )

    #  internal

    def _clear_defaults(self, user_id: int) -> None:
        update(
            self.conn,
            'addresses',
            {'is_default': 0},
            'user_id = ?',
            (user_id,),
        )


# ##
# Categories & Restaurants
# ##


class CategoryHandler:
    """CRUD operations for the ``categories`` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind the handler to *conn*."""
        self.conn = conn

    def create(
        self,
        *,
        name: str,
        description: str | None = None,
    ) -> int:
        """Insert a new category and return its id."""
        return insert(
            self.conn,
            'categories',
            {'name': name, 'description': description},
        )

    def list_all(self) -> list[dict]:
        """Return all categories sorted by name."""
        return fetch_all(
            self.conn,
            'SELECT * FROM categories ORDER BY name',
        )

    def get(self, category_id: int) -> dict | None:
        """Return a single category by primary key."""
        return fetch_one(
            self.conn,
            'SELECT * FROM categories WHERE category_id = ?',
            (category_id,),
        )


class RestaurantHandler:
    """CRUD and search for the ``restaurants`` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind the handler to *conn*."""
        self.conn = conn

    def create(
        self,
        *,
        name: str,
        street: str,
        city: str,
        category_id: int | None = None,
        rating: float = 0.0,
        delivery_fee: float = 0.0,
        min_order_amt: float = 0.0,
        is_open: int = 1,
    ) -> int:
        """Insert a new restaurant and return its id."""
        return insert(
            self.conn,
            'restaurants',
            {
                'category_id': category_id,
                'name': name,
                'street': street,
                'city': city,
                'rating': rating,
                'delivery_fee': delivery_fee,
                'min_order_amt': min_order_amt,
                'is_open': is_open,
            },
        )

    def get(self, restaurant_id: int) -> dict | None:
        """Return a restaurant joined with its category name."""
        return fetch_one(
            self.conn,
            'SELECT r.*, c.name AS category_name '
            'FROM restaurants r '
            'LEFT JOIN categories c USING (category_id) '
            'WHERE r.restaurant_id = ?',
            (restaurant_id,),
        )

    def search(
        self,
        *,
        city: str | None = None,
        category_id: int | None = None,
        is_open: bool | None = None,
        min_rating: float | None = None,
        query: str | None = None,
        page: int = 1,
        per_page: int = 20,
    ) -> dict:
        """Search restaurants with optional filters."""
        clauses: list[str] = []
        params: list[Any] = []

        if city:
            clauses.append('r.city = ?')
            params.append(city)
        if category_id is not None:
            clauses.append('r.category_id = ?')
            params.append(category_id)
        if is_open is not None:
            clauses.append('r.is_open = ?')
            params.append(int(is_open))
        if min_rating is not None:
            clauses.append('r.rating >= ?')
            params.append(min_rating)
        if query:
            clauses.append('r.name LIKE ?')
            params.append(f'%{query}%')

        where = ' AND '.join(clauses) if clauses else '1=1'
        sql = (
            'SELECT r.*, c.name AS category_name '  # noqa: S608 where is not user controlled
            'FROM restaurants r '
            'LEFT JOIN categories c USING (category_id) '
            f'WHERE {where} '
            'ORDER BY r.rating DESC'
        )
        return paginate(
            self.conn,
            sql,
            tuple(params),
            page=page,
            per_page=per_page,
        )

    def update(
        self,
        restaurant_id: int,
        **fields: str | float,
    ) -> int:
        """Update one or more columns on a restaurant."""
        return update(
            self.conn,
            'restaurants',
            fields,
            'restaurant_id = ?',
            (restaurant_id,),
        )

    def toggle_open(
        self,
        restaurant_id: int,
        *,
        is_open: bool,
    ) -> int:
        """Set the open/closed flag on a restaurant."""
        return self.update(
            restaurant_id,
            is_open=int(is_open),
        )


# ##
# Menu items
# ##


class MenuHandler:
    """CRUD operations for the ``menu_items`` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind the handler to *conn*."""
        self.conn = conn

    def create(
        self,
        *,
        restaurant_id: int,
        name: str,
        price: float,
        is_available: int = 1,
        is_vegetarian: int = 0,
        is_vegan: int = 0,
    ) -> int:
        """Insert a menu item and return its id."""
        return insert(
            self.conn,
            'menu_items',
            {
                'restaurant_id': restaurant_id,
                'name': name,
                'price': price,
                'is_available': is_available,
                'is_vegetarian': is_vegetarian,
                'is_vegan': is_vegan,
            },
        )

    def bulk_create(
        self,
        items: list[dict[str, Any]],
    ) -> int:
        """Insert multiple menu items at once."""
        return bulk_insert(self.conn, 'menu_items', items)

    def get(self, item_id: int) -> dict | None:
        """Return a single menu item by primary key."""
        return fetch_one(
            self.conn,
            'SELECT * FROM menu_items WHERE item_id = ?',
            (item_id,),
        )

    def list_for_restaurant(
        self,
        restaurant_id: int,
        *,
        available_only: bool = False,
        vegetarian: bool = False,
        vegan: bool = False,
    ) -> list[dict]:
        """List menu items with optional dietary filters."""
        clauses = ['restaurant_id = ?']
        params: list[Any] = [restaurant_id]
        if available_only:
            clauses.append('is_available = 1')
        if vegetarian:
            clauses.append('is_vegetarian = 1')
        if vegan:
            clauses.append('is_vegan = 1')
        where = ' AND '.join(clauses)
        return fetch_all(
            self.conn,
            'SELECT * FROM menu_items '  # noqa: S608
            f'WHERE {where} ORDER BY name',
            tuple(params),
        )

    def update(
        self,
        item_id: int,
        **fields: str | float,
    ) -> int:
        """Update one or more columns on a menu item."""
        return update(
            self.conn,
            'menu_items',
            fields,
            'item_id = ?',
            (item_id,),
        )

    def set_availability(
        self,
        item_id: int,
        *,
        available: bool,
    ) -> int:
        """Toggle availability on a menu item."""
        return self.update(
            item_id,
            is_available=int(available),
        )


# ##
# Orders & Order Items
# ##


class OrderHandler:
    """Handles creation, status transitions, and queries for orders."""

    VALID_TRANSITIONS: ClassVar[dict[str, list[str]]] = {
        'pending': ['confirmed', 'cancelled'],
        'confirmed': ['preparing', 'cancelled'],
        'preparing': ['ready', 'cancelled'],
        'ready': ['picked_up'],
        'picked_up': ['delivered'],
    }

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind the handler to *conn*."""
        self.conn = conn

    #  creation

    def create(
        self,
        *,
        user_id: int,
        restaurant_id: int,
        address_id: int,
        payment_method: str = 'card',
        items: list[dict[str, Any]],
    ) -> int:
        """Create an order with line items atomically.

        *items* must be a list of dicts with keys ``item_id`` and
        ``quantity``.  Prices are resolved server-side from
        ``menu_items`` to prevent client tampering.
        """
        rest = fetch_one(
            self.conn,
            'SELECT is_open, min_order_amt, delivery_fee '
            'FROM restaurants '
            'WHERE restaurant_id = ?',
            (restaurant_id,),
        )
        if not rest:
            msg = f'Restaurant {restaurant_id} not found'
            raise ValueError(msg)
        if not rest['is_open']:
            msg = 'Restaurant is currently closed'
            raise ValueError(msg)

        # Resolve prices
        line_items: list[dict[str, Any]] = []
        subtotal = 0.0
        for item in items:
            mi = fetch_one(
                self.conn,
                'SELECT price, is_available '
                'FROM menu_items '
                'WHERE item_id = ? '
                'AND restaurant_id = ?',
                (item['item_id'], restaurant_id),
            )
            if not mi:
                msg = (
                    f'Menu item {item["item_id"]} '
                    f'not found at restaurant '
                    f'{restaurant_id}'
                )
                raise ValueError(msg)
            if not mi['is_available']:
                msg = f'Menu item {item["item_id"]} is currently unavailable'
                raise ValueError(msg)
            line_total = mi['price'] * item['quantity']
            subtotal += line_total
            line_items.append(
                {
                    'item_id': item['item_id'],
                    'quantity': item['quantity'],
                    'unit_price': mi['price'],
                }
            )

        if subtotal < rest['min_order_amt']:
            msg = (
                f'Subtotal {subtotal:.2f} is below minimum {rest["min_order_amt"]:.2f}'
            )
            raise ValueError(msg)

        total_amount = subtotal + rest['delivery_fee']

        order_id = insert(
            self.conn,
            'orders',
            {
                'user_id': user_id,
                'restaurant_id': restaurant_id,
                'address_id': address_id,
                'status': 'pending',
                'subtotal': round(subtotal, 2),
                'total_amount': round(total_amount, 2),
                'payment_method': payment_method,
            },
        )

        for li in line_items:
            li['order_id'] = order_id
        bulk_insert(self.conn, 'order_items', line_items)

        return order_id

    #  status

    def advance_status(
        self,
        order_id: int,
        new_status: str,
    ) -> None:
        """Move an order to *new_status* if the transition is valid."""
        order = self.get(order_id)
        if not order:
            msg = f'Order {order_id} not found'
            raise ValueError(msg)
        current = order['status']
        allowed = self.VALID_TRANSITIONS.get(current, [])
        if new_status not in allowed:
            msg = f"Cannot transition from '{current}' to '{new_status}'"
            raise ValueError(msg)
        update(
            self.conn,
            'orders',
            {'status': new_status},
            'order_id = ?',
            (order_id,),
        )

    #  queries

    def get(self, order_id: int) -> dict | None:
        """Return a single order by primary key."""
        return fetch_one(
            self.conn,
            'SELECT * FROM orders WHERE order_id = ?',
            (order_id,),
        )

    def get_details(self, order_id: int) -> dict | None:
        """Return an order with line items and restaurant name."""
        order = fetch_one(
            self.conn,
            'SELECT o.*, r.name AS restaurant_name '
            'FROM orders o '
            'JOIN restaurants r USING (restaurant_id) '
            'WHERE o.order_id = ?',
            (order_id,),
        )
        if not order:
            return None
        order['items'] = fetch_all(
            self.conn,
            'SELECT oi.*, mi.name AS item_name '
            'FROM order_items oi '
            'JOIN menu_items mi USING (item_id) '
            'WHERE oi.order_id = ?',
            (order_id,),
        )
        return order

    def list_for_user(
        self,
        user_id: int,
        *,
        page: int = 1,
        per_page: int = 20,
    ) -> dict:
        """Return paginated orders for a user."""
        return paginate(
            self.conn,
            'SELECT * FROM orders WHERE user_id = ? ORDER BY ordered_at DESC',
            (user_id,),
            page=page,
            per_page=per_page,
        )

    def list_for_restaurant(
        self,
        restaurant_id: int,
        *,
        status: str | None = None,
        page: int = 1,
        per_page: int = 20,
    ) -> dict:
        """Return paginated orders for a restaurant."""
        params: list[Any] = [restaurant_id]
        status_clause = ''
        if status:
            status_clause = ' AND status = ?'
            params.append(status)
        return paginate(
            self.conn,
            'SELECT * FROM orders '  # noqa: S608
            'WHERE restaurant_id = ?'
            f'{status_clause} '
            'ORDER BY ordered_at DESC',
            tuple(params),
            page=page,
            per_page=per_page,
        )

    def active_count_for_restaurant(
        self,
        restaurant_id: int,
    ) -> int:
        """Count non-terminal orders for a restaurant."""
        return count(
            self.conn,
            'orders',
            "restaurant_id = ? AND status NOT IN ('delivered','cancelled')",
            (restaurant_id,),
        )


# ##
# Drivers & Deliveries
# ##


class DriverHandler:
    """CRUD operations for the ``drivers`` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind the handler to *conn*."""
        self.conn = conn

    def create(
        self,
        *,
        first_name: str,
        last_name: str,
        phone: str,
        vehicle_type: str = 'bicycle',
        rating: float = 5.0,
    ) -> int:
        """Insert a driver and return its id."""
        return insert(
            self.conn,
            'drivers',
            {
                'first_name': first_name,
                'last_name': last_name,
                'phone': phone,
                'vehicle_type': vehicle_type,
                'rating': rating,
            },
        )

    def get(self, driver_id: int) -> dict | None:
        """Return a single driver by primary key."""
        return fetch_one(
            self.conn,
            'SELECT * FROM drivers WHERE driver_id = ?',
            (driver_id,),
        )

    def update(
        self,
        driver_id: int,
        **fields: str | float,
    ) -> int:
        """Update one or more columns on a driver."""
        return update(
            self.conn,
            'drivers',
            fields,
            'driver_id = ?',
            (driver_id,),
        )

    def list_available(self) -> list[dict]:
        """Return drivers not on an active delivery."""
        return fetch_all(
            self.conn,
            'SELECT d.* '
            'FROM drivers d '
            'WHERE d.driver_id NOT IN ('
            '  SELECT driver_id FROM deliveries '
            '  WHERE delivered_at IS NULL'
            ') '
            'ORDER BY d.rating DESC',
        )


class DeliveryHandler:
    """CRUD operations for the ``deliveries`` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind the handler to *conn*."""
        self.conn = conn

    def assign(
        self,
        *,
        order_id: int,
        driver_id: int,
        estimated_mins: int | None = None,
        distance_km: float | None = None,
    ) -> int:
        """Assign a driver to an order."""
        return insert(
            self.conn,
            'deliveries',
            {
                'order_id': order_id,
                'driver_id': driver_id,
                'estimated_mins': estimated_mins,
                'distance_km': distance_km,
            },
        )

    def mark_picked_up(self, delivery_id: int) -> None:
        """Set ``picked_up_at`` to the current timestamp."""
        self.conn.execute(
            'UPDATE deliveries '
            "SET picked_up_at = datetime('now') "
            'WHERE delivery_id = ?',
            (delivery_id,),
        )
        self.conn.commit()

    def mark_delivered(self, delivery_id: int) -> None:
        """Set ``delivered_at`` to the current timestamp."""
        self.conn.execute(
            'UPDATE deliveries '
            "SET delivered_at = datetime('now') "
            'WHERE delivery_id = ?',
            (delivery_id,),
        )
        self.conn.commit()

    def get_for_order(self, order_id: int) -> dict | None:
        """Return the delivery for *order_id* with driver info."""
        return fetch_one(
            self.conn,
            'SELECT del.*, '
            "d.first_name || ' ' || d.last_name "
            'AS driver_name, '
            'd.phone AS driver_phone '
            'FROM deliveries del '
            'JOIN drivers d USING (driver_id) '
            'WHERE del.order_id = ?',
            (order_id,),
        )

    def active_for_driver(
        self,
        driver_id: int,
    ) -> list[dict]:
        """Return unfinished deliveries for a driver."""
        return fetch_all(
            self.conn,
            'SELECT del.*, o.status AS order_status '
            'FROM deliveries del '
            'JOIN orders o USING (order_id) '
            'WHERE del.driver_id = ? '
            'AND del.delivered_at IS NULL',
            (driver_id,),
        )


# ##
# Reviews
# ##


class ReviewHandler:
    """CRUD operations for the ``reviews`` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind the handler to *conn*."""
        self.conn = conn

    def create(
        self,
        *,
        order_id: int,
        user_id: int,
        restaurant_id: int,
        food_rating: int,
        delivery_rating: int,
        comment: str | None = None,
    ) -> int:
        """Submit a review for a delivered order.

        Validates ownership, delivery status, and duplicate
        prevention before inserting.
        """
        order = fetch_one(
            self.conn,
            'SELECT status, user_id FROM orders WHERE order_id = ?',
            (order_id,),
        )
        if not order:
            msg = f'Order {order_id} not found'
            raise ValueError(msg)
        if order['user_id'] != user_id:
            msg = "Cannot review someone else's order"
            raise ValueError(msg)
        if order['status'] != 'delivered':
            msg = 'Can only review delivered orders'
            raise ValueError(msg)
        if exists(
            self.conn,
            'reviews',
            'order_id = ?',
            (order_id,),
        ):
            msg = 'Order already reviewed'
            raise ValueError(msg)

        review_id = insert(
            self.conn,
            'reviews',
            {
                'order_id': order_id,
                'user_id': user_id,
                'restaurant_id': restaurant_id,
                'food_rating': food_rating,
                'delivery_rating': delivery_rating,
                'comment': comment,
            },
        )

        self._refresh_restaurant_rating(restaurant_id)
        return review_id

    def list_for_restaurant(
        self,
        restaurant_id: int,
        *,
        page: int = 1,
        per_page: int = 20,
    ) -> dict:
        """Return paginated reviews for a restaurant."""
        return paginate(
            self.conn,
            'SELECT rv.*, '
            "u.first_name || ' ' || u.last_name "
            'AS reviewer_name '
            'FROM reviews rv '
            'JOIN users u USING (user_id) '
            'WHERE rv.restaurant_id = ? '
            'ORDER BY rv.review_id DESC',
            (restaurant_id,),
            page=page,
            per_page=per_page,
        )

    def average_ratings(
        self,
        restaurant_id: int,
    ) -> dict:
        """Return average food/delivery ratings and count."""
        row = fetch_one(
            self.conn,
            'SELECT '
            'AVG(food_rating) AS avg_food, '
            'AVG(delivery_rating) AS avg_delivery, '
            'COUNT(*) AS total_reviews '
            'FROM reviews WHERE restaurant_id = ?',
            (restaurant_id,),
        )
        return row or {
            'avg_food': None,
            'avg_delivery': None,
            'total_reviews': 0,
        }

    #  internal

    def _refresh_restaurant_rating(
        self,
        restaurant_id: int,
    ) -> None:
        avg = self.average_ratings(restaurant_id)
        if avg['avg_food'] is not None:
            combined = round(
                (avg['avg_food'] + avg['avg_delivery']) / 2,
                2,
            )
            update(
                self.conn,
                'restaurants',
                {'rating': combined},
                'restaurant_id = ?',
                (restaurant_id,),
            )
