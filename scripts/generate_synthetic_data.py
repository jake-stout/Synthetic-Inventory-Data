import os
import json
import random
import logging
from datetime import datetime, timedelta
from uuid import uuid4

from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "inventory")
DB_USER = os.getenv("DB_USER", "inventory")
DB_PASSWORD = os.getenv("DB_PASSWORD", "inventory")
EVENT_FILE = os.getenv("EVENT_FILE", "inventory_events.json")

fake = Faker()
random.seed(42)

TRANSACTION_TYPES = ["RECEIPT", "ISSUE", "SCRAP", "TRANSFER_IN", "TRANSFER_OUT", "ADJUSTMENT"]


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def load_table(conn, sql, data):
    with conn.cursor() as cur:
        try:
            execute_batch(cur, sql, data)
            conn.commit()
        except Exception:
            conn.rollback()
            logging.exception("Error executing batch for SQL: %s", sql)
            raise


# ---------------------------------------------------------------------------
# Data generation utilities
# ---------------------------------------------------------------------------

def generate_part_master(n=50):
    parts = []
    for _ in range(n):
        part_number = f"PN{fake.unique.bothify(text='#####') }"
        part_description = fake.catch_phrase()
        part_condition = random.choice(["NEW", "SERVICEABLE", "REPAIRED"])
        unit_of_measure = random.choice(["EA", "SET", "KIT"])
        standard_cost = round(random.uniform(100, 10000), 2)
        manufacturer = fake.company()
        category = random.choice(["AIRFRAME", "ENGINE", "AVIONICS", "INTERIOR"])
        parts.append((
            part_number,
            part_description,
            part_condition,
            unit_of_measure,
            standard_cost,
            manufacturer,
            category,
        ))
    return parts


def generate_facilities(n=5):
    facilities = []
    for _ in range(n):
        code = fake.unique.bothify(text='FAC##')
        facilities.append((
            code,
            fake.company(),
            fake.street_address(),
            fake.city(),
            fake.state_abbr(),
            fake.country(),
        ))
    return facilities


def generate_users(n=10):
    users = []
    for _ in range(n):
        users.append((
            fake.name(),
            fake.unique.email(),
            random.choice(["CLERK", "SUPERVISOR", "MANAGER"]),
        ))
    return users


def generate_vendors(n=10):
    vendors = []
    for _ in range(n):
        vendors.append((
            fake.company(),
            fake.address().replace("\n", ", "),
            fake.company_email(),
        ))
    return vendors


def generate_work_orders(n=20):
    orders = []
    for _ in range(n):
        number = fake.unique.bothify(text='WO#####')
        started = fake.date_time_between(start_date='-120d', end_date='-30d')
        closed = started + timedelta(days=random.randint(1, 30))
        posted = closed + timedelta(days=random.randint(1, 10))
        orders.append((
            number,
            fake.city(),
            random.choice(["OPEN", "CLOSED", "POSTED"]),
            fake.company(),
            random.choice(["G650", "G700", "G800"]),
            fake.bothify(text='N#####'),
            started,
            closed,
            posted,
            fake.address().replace("\n", ", "),
        ))
    return orders


# ---------------------------------------------------------------------------
# Inventory transaction generation
# ---------------------------------------------------------------------------

def nested_bin():
    return f"A{random.randint(1,3)}-R{random.randint(1,5)}-S{random.randint(1,5)}"


def random_lot():
    return f"LOT{random.randint(1,20):03d}"


def generate_transactions(parts, facilities, work_orders, users, count=1000):
    inventory = {}
    events = []

    part_numbers = [p[0] for p in parts]
    facility_codes = [f[0] for f in facilities]
    work_order_numbers = [w[0] for w in work_orders]

    current_time = datetime.utcnow()
    start_time = current_time - timedelta(days=60)

    timestamps = [start_time + timedelta(seconds=i*(60*60*24*60)//count) for i in range(count)]

    for ts in timestamps:
        t_type = random.choices(
            population=TRANSACTION_TYPES,
            weights=[0.4, 0.3, 0.05, 0.1, 0.1, 0.05],
            k=1,
        )[0]

        part = random.choice(part_numbers)
        lot = random_lot()
        warehouse = random.choice(["ATL", "LAX", "DFW", "SEA"])
        bin_loc = nested_bin()
        source = random.choice(facility_codes)
        dest = random.choice(facility_codes)
        quantity = random.randint(1, 5)
        unit_cost = round(random.uniform(100, 10000), 2)
        user_id = random.randint(1, len(users))

        key = (part, lot, warehouse, bin_loc)
        on_hand = inventory.get(key, 0)

        if t_type == "RECEIPT":
            inventory[key] = on_hand + quantity
        elif t_type == "ISSUE":
            if on_hand == 0:
                # no stock, convert to receipt to seed inventory
                t_type = "RECEIPT"
                inventory[key] = on_hand + quantity
            else:
                qty = min(quantity, on_hand)
                quantity = qty
                inventory[key] = on_hand - qty
        elif t_type == "SCRAP":
            if on_hand == 0:
                continue  # skip invalid scrap
            qty = min(quantity, on_hand)
            quantity = qty
            inventory[key] = on_hand - qty
        elif t_type == "ADJUSTMENT":
            # small adjustments up or down but never negative inventory
            adj = random.randint(-2, 2)
            if on_hand + adj < 0:
                adj = -on_hand
            quantity = adj
            inventory[key] = on_hand + adj
        elif t_type in ("TRANSFER_IN", "TRANSFER_OUT"):
            # to ensure paired transfers, generate TRANSFER_IN then TRANSFER_OUT
            if on_hand == 0:
                continue
            qty = min(quantity, on_hand)
            quantity = qty
            inventory[key] = on_hand - qty
            # destination event
            dest_key = (part, lot, dest, nested_bin())
            inventory[dest_key] = inventory.get(dest_key, 0) + qty
            events.append({
                "transaction_id": str(uuid4()),
                "transaction_type": "TRANSFER_IN",
                "part_number": part,
                "lot_number": lot,
                "warehouse": dest,
                "bin": dest_key[3],
                "source_facility": source,
                "destination_facility": dest,
                "quantity": qty,
                "unit_cost": unit_cost,
                "total_cost": round(qty * unit_cost, 2),
                "work_order_number": None,
                "transaction_timestamp": ts.isoformat(),
                "created_by": user_id,
            })
            t_type = "TRANSFER_OUT"
        # record main event
        event = {
            "transaction_id": str(uuid4()),
            "transaction_type": t_type,
            "part_number": part,
            "lot_number": lot,
            "warehouse": warehouse,
            "bin": bin_loc,
            "source_facility": source,
            "destination_facility": dest,
            "quantity": quantity,
            "unit_cost": unit_cost,
            "total_cost": round(quantity * unit_cost, 2),
            "work_order_number": random.choice(work_order_numbers) if t_type == "ISSUE" else None,
            "transaction_timestamp": ts.isoformat(),
            "created_by": user_id,
        }
        events.append(event)

    return events, inventory


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

def main():
    conn = get_connection()

    parts = generate_part_master()
    facilities = generate_facilities()
    users = generate_users()
    vendors = generate_vendors()
    work_orders = generate_work_orders()

    load_table(conn, """
        INSERT INTO part_master (
            part_number, part_description, part_condition, unit_of_measure,
            standard_cost, manufacturer, category)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, parts)

    load_table(conn, """
        INSERT INTO facilities (
            facility_code, facility_name, address, city, state, country)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, facilities)

    load_table(conn, """
        INSERT INTO users (name, email, role) VALUES (%s,%s,%s)
    """, users)

    load_table(conn, """
        INSERT INTO vendors (name, address, contact_email) VALUES (%s,%s,%s)
    """, vendors)

    load_table(conn, """
        INSERT INTO work_orders (
            work_order_number, site, status, customer, aircraft_type,
            aircraft_tail, started, closed, posted, facility_address)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, work_orders)

    events, inventory = generate_transactions(parts, facilities, work_orders, users)

    # insert transactions
    tx_rows = [(
        e["transaction_id"], e["transaction_type"], e["part_number"], e["lot_number"],
        e["warehouse"], e["bin"], e["source_facility"], e["destination_facility"],
        e["quantity"], e["unit_cost"], e["work_order_number"], e["transaction_timestamp"],
        e["created_by"],
    ) for e in events]

    load_table(conn, """
        INSERT INTO inventory_transactions (
            transaction_id, transaction_type, part_number, lot_number, warehouse,
            bin, source_facility, destination_facility, quantity, unit_cost,
            work_order_number, transaction_timestamp, created_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, tx_rows)

    # inventory on hand snapshot
    onhand_rows = [(
        k[0], k[1], k[2], k[3], qty, datetime.utcnow()
    ) for k, qty in inventory.items()]

    load_table(conn, """
        INSERT INTO inventory_on_hand (
            part_number, lot_number, warehouse, bin, quantity_on_hand, last_updated)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, onhand_rows)

    conn.close()

    # write events to json file
    with open(EVENT_FILE, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    print(f"Generated {len(events)} transactions to {EVENT_FILE}")


if __name__ == "__main__":
    main()
