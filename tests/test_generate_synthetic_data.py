from scripts.generate_synthetic_data import (
    generate_part_master,
    generate_facilities,
    generate_users,
    generate_work_orders,
    generate_transactions,
)


def test_generate_part_master_unique():
    parts = generate_part_master(10)
    numbers = [p[0] for p in parts]
    assert len(numbers) == len(set(numbers)) == 10


def test_generate_transactions_inventory_non_negative():
    parts = generate_part_master()
    facilities = generate_facilities()
    users = generate_users()
    work_orders = generate_work_orders()
    events, inventory = generate_transactions(parts, facilities, work_orders, users, count=50)
    assert all(qty >= 0 for qty in inventory.values())


def test_generate_transactions_total_cost_matches_quantity():
    parts = generate_part_master()
    facilities = generate_facilities()
    users = generate_users()
    work_orders = generate_work_orders()
    events, _ = generate_transactions(parts, facilities, work_orders, users, count=50)
    for e in events:
        assert e["total_cost"] == round(e["quantity"] * e["unit_cost"], 2)
