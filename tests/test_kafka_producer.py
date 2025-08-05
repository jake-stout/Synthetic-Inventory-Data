from scripts.kafka_producer import generate_transaction


def test_generate_transaction_total_cost():
    tx = generate_transaction()
    assert tx["total_cost"] == round(tx["quantity"] * tx["unit_cost"], 2)
