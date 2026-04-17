from __future__ import annotations

import argparse
import logging
import random
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from faker import Faker

from db_utils.db import DB_PATH, get_db, init_db

log = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)


_CANCEL_PROBABILITY = 0.05
_REVIEW_PROBABILITY = 0.60
_COMMENT_PROBABILITY = 0.7


CATEGORIES = [
    ('Pizza', 'Classic Italian pizzas and calzones'),
    ('Sushi', 'Fresh Japanese sushi and sashimi'),
    ('Burgers', 'Gourmet and classic burgers'),
    ('Thai', 'Authentic Thai curries and noodles'),
    ('Indian', 'Curries, biryanis, and tandoori dishes'),
    ('Chinese', 'Dim sum, stir-fries, and noodle soups'),
    ('Mexican', 'Tacos, burritos, and enchiladas'),
    ('Vegetarian', 'Plant-based meals and salads'),
    ('Desserts', 'Cakes, pastries, and ice cream'),
    ('Kebab', 'Grilled meats and wraps'),
    ('Seafood', 'Fish, shrimp, and shellfish dishes'),
    ('Breakfast', 'Morning favourites and brunch'),
]

MENU_TEMPLATES: dict[str, list[tuple[str, float, bool, bool]]] = {
    'Pizza': [
        ('Margherita', 12.0, True, False),
        ('Pepperoni', 14.0, False, False),
        ('Quattro Formaggi', 15.0, True, False),
        ('Diavola', 14.5, False, False),
        ('Veggie Supreme', 13.5, True, True),
        ('Hawaiian', 13.0, False, False),
        ('Calzone', 15.5, False, False),
        ('Garlic Bread', 6.0, True, True),
    ],
    'Sushi': [
        ('Salmon Nigiri (6pc)', 11.0, False, False),
        ('Tuna Sashimi (8pc)', 14.0, False, False),
        ('California Roll (8pc)', 10.0, False, False),
        ('Veggie Roll (8pc)', 9.0, True, True),
        ('Dragon Roll', 16.0, False, False),
        ('Miso Soup', 4.5, True, True),
        ('Edamame', 5.0, True, True),
    ],
    'Burgers': [
        ('Classic Cheeseburger', 11.0, False, False),
        ('Bacon BBQ Burger', 13.5, False, False),
        ('Mushroom Swiss Burger', 12.5, False, False),
        ('Veggie Burger', 11.0, True, True),
        ('Chicken Burger', 12.0, False, False),
        ('Fries (large)', 5.0, True, True),
        ('Onion Rings', 5.5, True, True),
        ('Milkshake', 6.0, True, False),
    ],
    'Thai': [
        ('Pad Thai', 12.0, False, False),
        ('Green Curry', 13.0, False, False),
        ('Massaman Curry', 13.5, False, False),
        ('Tom Yum Soup', 10.0, False, False),
        ('Veggie Pad Thai', 11.5, True, True),
        ('Spring Rolls (4pc)', 7.0, True, True),
        ('Mango Sticky Rice', 8.0, True, True),
    ],
    'Indian': [
        ('Butter Chicken', 13.0, False, False),
        ('Lamb Biryani', 15.0, False, False),
        ('Palak Paneer', 12.0, True, False),
        ('Chana Masala', 11.0, True, True),
        ('Tandoori Chicken', 14.0, False, False),
        ('Garlic Naan', 3.5, True, False),
        ('Samosa (2pc)', 5.0, True, True),
        ('Mango Lassi', 4.5, True, False),
    ],
    'Chinese': [
        ('Kung Pao Chicken', 12.5, False, False),
        ('Sweet & Sour Pork', 13.0, False, False),
        ('Mapo Tofu', 11.0, True, True),
        ('Fried Rice', 9.5, False, False),
        ('Dim Sum Basket (6pc)', 10.0, False, False),
        ('Wonton Soup', 8.0, False, False),
        ('Spring Rolls (4pc)', 6.5, True, True),
    ],
    'Mexican': [
        ('Beef Burrito', 12.0, False, False),
        ('Chicken Tacos (3pc)', 11.0, False, False),
        ('Veggie Quesadilla', 10.0, True, False),
        ('Nachos Supreme', 11.5, False, False),
        ('Guacamole & Chips', 7.0, True, True),
        ('Churros (4pc)', 6.0, True, False),
        ('Elote (street corn)', 5.5, True, False),
    ],
    'Vegetarian': [
        ('Buddha Bowl', 13.0, True, True),
        ('Halloumi Wrap', 11.5, True, False),
        ('Falafel Plate', 12.0, True, True),
        ('Caprese Salad', 10.0, True, False),
        ('Veggie Stir-Fry', 11.0, True, True),
        ('Smoothie Bowl', 9.5, True, True),
        ('Sweet Potato Fries', 6.0, True, True),
    ],
    'Desserts': [
        ('Chocolate Lava Cake', 9.0, True, False),
        ('Tiramisu', 10.0, True, False),
        ('Cheesecake Slice', 8.5, True, False),
        ('Crème Brûlée', 9.5, True, False),
        ('Brownie Sundae', 8.0, True, False),
        ('Fruit Sorbet (3 scoops)', 7.0, True, True),
        ('Vegan Banana Bread', 6.5, True, True),
    ],
    'Kebab': [
        ('Chicken Shawarma Wrap', 11.0, False, False),
        ('Lamb Kebab Plate', 14.0, False, False),
        ('Mixed Grill', 16.0, False, False),
        ('Falafel Wrap', 10.0, True, True),
        ('Hummus & Pita', 6.5, True, True),
        ('Fattoush Salad', 7.0, True, True),
    ],
    'Seafood': [
        ('Fish & Chips', 13.0, False, False),
        ('Grilled Salmon', 17.0, False, False),
        ('Shrimp Scampi', 16.0, False, False),
        ('Lobster Roll', 19.0, False, False),
        ('Clam Chowder', 9.5, False, False),
        ('Calamari (fried)', 8.5, False, False),
    ],
    'Breakfast': [
        ('Full English', 12.0, False, False),
        ('Avocado Toast', 9.5, True, True),
        ('Pancake Stack', 10.0, True, False),
        ('Eggs Benedict', 12.5, False, False),
        ('Açaí Bowl', 11.0, True, True),
        ('Croissant & Jam', 5.5, True, False),
        ('Granola Parfait', 8.0, True, False),
    ],
}

RESTAURANT_NAME_PARTS = {
    'Pizza': (
        ['Bella', "Tony's", 'Roma', 'Napoli', 'Vesuvio', 'Il Forno'],
        ['Pizzeria', 'Pizza', 'Slice House'],
    ),
    'Sushi': (
        ['Sakura', 'Tokyo', 'Zen', "Hiro's", 'Koi', 'Wasabi'],
        ['Sushi', 'Japanese Kitchen', 'Sushi Bar'],
    ),
    'Burgers': (
        ['Big', 'Smoky', 'Prime', 'Stack', 'Flame'],
        ['Burger Co.', 'Burgers', 'Grill', 'Burger Bar'],
    ),
    'Thai': (
        ['Golden', 'Siam', 'Bangkok', 'Lotus', 'Spice'],
        ['Thai', 'Thai Kitchen', 'Thai Bistro'],
    ),
    'Indian': (
        ['Masala', 'Taj', 'Saffron', 'Namaste', 'Spice Route'],
        ['Kitchen', 'Indian Grill', 'Tandoori'],
    ),
    'Chinese': (
        ['Golden', 'Dragon', 'Jade', 'Lucky', 'Bamboo'],
        ['Palace', 'Garden', 'Wok', 'Kitchen'],
    ),
    'Mexican': (
        ['El', 'Casa', 'Loco', 'Sol', 'Fiesta'],
        ['Taqueria', 'Cantina', 'Grill', 'Mexican'],
    ),
    'Vegetarian': (
        ['Green', 'Fresh', 'Sprout', 'Root', 'Leaf'],
        ['Kitchen', 'Bowl', 'Café', 'Eatery'],
    ),
    'Desserts': (
        ['Sweet', 'Sugar', 'Crumbs', 'Bliss', 'Honeycomb'],
        ['Bakery', 'Desserts', 'Patisserie'],
    ),
    'Kebab': (
        ['Istanbul', 'Anatolian', "Sultan's", 'Flame', 'Meze'],
        ['Kebab', 'Grill', 'Kitchen'],
    ),
    'Seafood': (
        ['Ocean', 'Harbour', 'Blue', 'Catch', "Neptune's"],
        ['Seafood', 'Fish Bar', 'Grill'],
    ),
    'Breakfast': (
        ['Sunrise', 'Morning', 'Early', 'Golden', 'Rise &'],
        ['Café', 'Brunch', 'Breakfast Club'],
    ),
}

CITIES = [
    'Oslo',
    'Bergen',
    'Trondheim',
    'Stavanger',
    'Tromsø',
]
ADDRESS_LABELS = [
    'Home',
    'Work',
    "Partner's Place",
    'Gym',
    'Office',
]
VEHICLE_TYPES = ['bicycle', 'scooter', 'car']
PAYMENT_METHODS = ['card', 'cash', 'wallet']
ORDER_STATUSES_FULL = [
    'pending',
    'confirmed',
    'preparing',
    'ready',
    'picked_up',
    'delivered',
]


# ═══════════════════════════════════════════════════════════════════
# Seeders
# ═══════════════════════════════════════════════════════════════════


def seed_categories(
    conn: sqlite3.Connection,
) -> dict[str, int]:
    """Insert categories and return ``{name: category_id}``."""
    mapping: dict[str, int] = {}
    for name, desc in CATEGORIES:
        cur = conn.execute(
            'INSERT INTO categories (name, description) VALUES (?, ?)',
            (name, desc),
        )
        mapping[name] = cur.lastrowid
    conn.commit()
    log.info('%d categories', len(mapping))
    return mapping


def seed_users(
    conn: sqlite3.Connection,
    n: int,
) -> list[int]:
    """Create *n* users, each with 1-3 addresses."""
    user_ids: list[int] = []
    for _ in range(n):
        cur = conn.execute(
            'INSERT INTO users '
            '(first_name, last_name, email, phone) '
            'VALUES (?, ?, ?, ?)',
            (
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.phone_number(),
            ),
        )
        uid = cur.lastrowid
        user_ids.append(uid)

        num_addrs = random.randint(1, 3)  # noqa: S311
        for j in range(num_addrs):
            conn.execute(
                'INSERT INTO addresses '
                '(user_id, label, street, '
                'city, postal_code, is_default) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (
                    uid,
                    ADDRESS_LABELS[j % len(ADDRESS_LABELS)],
                    fake.street_address(),
                    random.choice(CITIES),  # noqa: S311
                    fake.postcode(),
                    1 if j == 0 else 0,
                ),
            )
    conn.commit()
    log.info('%d users (with addresses)', n)
    return user_ids


def seed_restaurants(
    conn: sqlite3.Connection,
    n: int,
    category_map: dict[str, int],
) -> list[dict]:
    """Create *n* restaurants with full menus."""
    restaurants: list[dict] = []
    cat_names = list(category_map.keys())
    used_names: set[str] = set()

    for _ in range(n):
        cat_name = random.choice(cat_names)  # noqa: S311
        cat_id = category_map[cat_name]

        parts = RESTAURANT_NAME_PARTS.get(
            cat_name,
            (['The'], ['Eatery']),
        )
        for _attempt in range(20):
            first = random.choice(parts[0])  # noqa: S311
            second = random.choice(parts[1])  # noqa: S311
            rname = f'{first} {second}'
            if rname not in used_names:
                used_names.add(rname)
                break
        else:
            last = random.choice(parts[1])  # noqa: S311
            rname = f"{fake.last_name()}'s {last}"

        city = random.choice(CITIES)  # noqa: S311
        cur = conn.execute(
            'INSERT INTO restaurants '
            '(category_id, name, street, city, '
            'rating, delivery_fee, '
            'min_order_amt, is_open) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (
                cat_id,
                rname,
                fake.street_address(),
                city,
                round(
                    random.uniform(3.0, 5.0),  # noqa: S311
                    1,
                ),
                round(
                    random.choice(  # noqa: S311
                        [0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0],
                    ),
                    2,
                ),
                round(
                    random.choice(  # noqa: S311
                        [0, 8, 10, 12, 15],
                    ),
                    2,
                ),
                random.choices(  # noqa: S311
                    [1, 0],
                    weights=[85, 15],
                )[0],
            ),
        )
        rid = cur.lastrowid

        template = MENU_TEMPLATES.get(
            cat_name,
            MENU_TEMPLATES['Burgers'],
        )
        item_ids: list[int] = []
        for item_name, base_price, is_veg, is_vgn in template:
            price = round(
                base_price + random.uniform(-1.0, 2.0),  # noqa: S311
                2,
            )
            price = max(price, 2.0)
            ic = conn.execute(
                'INSERT INTO menu_items '
                '(restaurant_id, name, price, '
                'is_available, is_vegetarian, is_vegan) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (
                    rid,
                    item_name,
                    price,
                    random.choices(  # noqa: S311
                        [1, 0],
                        weights=[90, 10],
                    )[0],
                    int(is_veg),
                    int(is_vgn),
                ),
            )
            item_ids.append(ic.lastrowid)

        restaurants.append(
            {
                'id': rid,
                'category': cat_name,
                'item_ids': item_ids,
            }
        )

    conn.commit()
    log.info('%d restaurants (with menus)', n)
    return restaurants


def seed_drivers(
    conn: sqlite3.Connection,
    n: int,
) -> list[int]:
    """Create *n* drivers."""
    driver_ids: list[int] = []
    for _ in range(n):
        cur = conn.execute(
            'INSERT INTO drivers '
            '(first_name, last_name, phone, '
            'vehicle_type, rating) '
            'VALUES (?, ?, ?, ?, ?)',
            (
                fake.first_name(),
                fake.last_name(),
                fake.phone_number(),
                random.choice(VEHICLE_TYPES),  # noqa: S311
                round(
                    random.uniform(3.5, 5.0),  # noqa: S311
                    1,
                ),
            ),
        )
        driver_ids.append(cur.lastrowid)
    conn.commit()
    log.info('%d drivers', n)
    return driver_ids


def seed_orders(
    conn: sqlite3.Connection,
    n: int,
    user_ids: list[int],
    restaurants: list[dict],
    driver_ids: list[int],
) -> None:
    """Create *n* orders with line items, deliveries, reviews."""
    review_count = 0
    delivery_count = 0

    for _ in range(n):
        uid = random.choice(user_ids)  # noqa: S311

        addr = conn.execute(
            'SELECT address_id FROM addresses '
            'WHERE user_id = ? '
            'ORDER BY is_default DESC LIMIT 1',
            (uid,),
        ).fetchone()
        if not addr:
            continue
        aid = addr['address_id']

        rest = random.choice(restaurants)  # noqa: S311
        rid = rest['id']

        num_items = random.randint(  # noqa: S311
            1,
            min(4, len(rest['item_ids'])),
        )
        chosen_items = random.sample(
            rest['item_ids'],
            num_items,
        )

        subtotal = 0.0
        line_items: list[tuple] = []
        for item_id in chosen_items:
            mi = conn.execute(
                'SELECT price FROM menu_items WHERE item_id = ?',
                (item_id,),
            ).fetchone()
            qty = random.randint(1, 3)  # noqa: S311
            unit_price = mi['price']
            subtotal += unit_price * qty
            line_items.append((item_id, qty, unit_price))

        dfr = conn.execute(
            'SELECT delivery_fee FROM restaurants WHERE restaurant_id = ?',
            (rid,),
        ).fetchone()
        delivery_fee = dfr['delivery_fee'] if dfr else 0.0
        total = round(subtotal + delivery_fee, 2)

        if random.random() < _CANCEL_PROBABILITY:  # noqa: S311
            final_status = 'cancelled'
        else:
            stop = random.choices(  # noqa: S311
                range(len(ORDER_STATUSES_FULL)),
                weights=[3, 3, 5, 5, 5, 79],
            )[0]
            final_status = ORDER_STATUSES_FULL[stop]

        ordered_at = fake.date_time_between(
            start_date='-90d',
            end_date='now',
        )

        cur = conn.execute(
            'INSERT INTO orders '
            '(user_id, restaurant_id, address_id, '
            'status, subtotal, total_amount, '
            'payment_method, ordered_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (
                uid,
                rid,
                aid,
                final_status,
                round(subtotal, 2),
                total,
                random.choice(PAYMENT_METHODS),  # noqa: S311
                ordered_at.isoformat(),
            ),
        )
        oid = cur.lastrowid

        for item_id, qty, unit_price in line_items:
            conn.execute(
                'INSERT INTO order_items '
                '(order_id, item_id, quantity, '
                'unit_price) '
                'VALUES (?, ?, ?, ?)',
                (oid, item_id, qty, unit_price),
            )

        if final_status in ('picked_up', 'delivered'):
            did = random.choice(driver_ids)  # noqa: S311
            est = random.randint(15, 55)  # noqa: S311
            dist = round(
                random.uniform(0.5, 12.0),  # noqa: S311
                1,
            )
            picked = fake.date_time_between(
                start_date=ordered_at,
                end_date='now',
            )
            delivered_at = (
                fake.date_time_between(
                    start_date=picked,
                    end_date='now',
                )
                if final_status == 'delivered'
                else None
            )
            conn.execute(
                'INSERT INTO deliveries '
                '(order_id, driver_id, picked_up_at, '
                'delivered_at, estimated_mins, '
                'distance_km) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (
                    oid,
                    did,
                    picked.isoformat(),
                    (delivered_at.isoformat() if delivered_at else None),
                    est,
                    dist,
                ),
            )
            delivery_count += 1

        if (
            final_status == 'delivered' and random.random() < _REVIEW_PROBABILITY  # noqa: S311
        ):
            conn.execute(
                'INSERT INTO reviews '
                '(order_id, user_id, restaurant_id, '
                'food_rating, delivery_rating, comment) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (
                    oid,
                    uid,
                    rid,
                    random.randint(1, 5),  # noqa: S311
                    random.randint(1, 5),  # noqa: S311
                    (
                        fake.sentence(
                            nb_words=random.randint(  # noqa: S311
                                4,
                                15,
                            ),
                        )
                        if random.random()  # noqa: S311
                        < _COMMENT_PROBABILITY
                        else None
                    ),
                ),
            )
            review_count += 1

    conn.commit()
    log.info(
        '%d orders (%d deliveries, %d reviews)',
        n,
        delivery_count,
        review_count,
    )


def refresh_restaurant_ratings(
    conn: sqlite3.Connection,
) -> None:
    """Recompute each restaurant's rating from reviews."""
    conn.execute("""
        UPDATE restaurants
        SET rating = COALESCE((
            SELECT ROUND(
                AVG((food_rating + delivery_rating) / 2.0),
                1
            )
            FROM reviews
            WHERE reviews.restaurant_id
                  = restaurants.restaurant_id
        ), rating)
    """)
    conn.commit()
    log.info('Restaurant ratings recalculated')


# ═══════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════


def seed(
    *,
    num_users: int = 50,
    num_restaurants: int = 15,
    num_orders: int = 200,
    num_drivers: int | None = None,
    reset: bool = False,
) -> None:
    """Run the full seeding pipeline."""
    if reset:
        p = Path(DB_PATH)
        if p.exists():
            p.unlink()
            log.info('Deleted %s', DB_PATH)

    init_db()
    log.info('Seeding %s …', DB_PATH)

    with get_db() as conn:
        cat_map = seed_categories(conn)
        user_ids = seed_users(conn, num_users)
        restaurants = seed_restaurants(
            conn,
            num_restaurants,
            cat_map,
        )
        drivers_n = num_drivers or max(
            5,
            num_orders // 15,
        )
        driver_ids = seed_drivers(conn, drivers_n)
        seed_orders(
            conn,
            num_orders,
            user_ids,
            restaurants,
            driver_ids,
        )
        refresh_restaurant_ratings(conn)

    log.info('Done. Database ready at %s', DB_PATH)


def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='  %(message)s',
    )

    parser = argparse.ArgumentParser(
        description='Seed the food-delivery database',
    )
    parser.add_argument(
        '--users',
        type=int,
        default=50,
        help='Number of users (default 50)',
    )
    parser.add_argument(
        '--restaurants',
        type=int,
        default=15,
        help='Number of restaurants (default 15)',
    )
    parser.add_argument(
        '--orders',
        type=int,
        default=200,
        help='Number of orders (default 200)',
    )
    parser.add_argument(
        '--drivers',
        type=int,
        default=None,
        help='Number of drivers (auto if omitted)',
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Delete existing DB before seeding',
    )
    args = parser.parse_args()

    seed(
        num_users=args.users,
        num_restaurants=args.restaurants,
        num_orders=args.orders,
        num_drivers=args.drivers,
        reset=args.reset,
    )


if __name__ == '__main__':
    main()
