import json
import os
import time
import uuid
import random
from datetime import datetime

from faker import Faker
from confluent_kafka import Producer

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "inventory_transactions")

TRANSACTION_TYPES = [
    "RECEIPT",
    "ISSUE",
    "SCRAP",
    "TRANSFER_IN",
    "TRANSFER_OUT",
    "ADJUSTMENT",
]


def generate_transaction():
    quantity = random.randint(1, 100)
    unit_cost = round(random.uniform(10.0, 1000.0), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "part_number": f"PART-{random.randint(1000, 9999)}",
        "lot_number": f"LOT-{random.randint(1000, 9999)}",
        "warehouse": f"WH{random.randint(1, 5)}",
        "bin": f"BIN-{random.randint(1, 100)}",
        "source_facility": f"FAC-{random.randint(1, 5)}",
        "destination_facility": f"FAC-{random.randint(1, 5)}",
        "quantity": quantity,
        "unit_cost": unit_cost,
        "total_cost": round(quantity * unit_cost, 2),
        "work_order_number": f"WO-{random.randint(10000, 99999)}",
        "transaction_timestamp": datetime.utcnow().isoformat(),
        "created_by": random.randint(1, 10),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"Produced record to {msg.topic()} partition {msg.partition()} @ offset {msg.offset()}"
        )


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    try:
        while True:
            transaction = generate_transaction()
            producer.produce(
                KAFKA_TOPIC, json.dumps(transaction), callback=delivery_report
            )
            producer.poll(0)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
