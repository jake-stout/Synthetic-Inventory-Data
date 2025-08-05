import os
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "inventory_events")
EVENT_FILE = os.getenv("EVENT_FILE", "inventory_events.json")


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        print(f"Produced record to {msg.topic()} partition {msg.partition()} @ offset {msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    try:
        with open(EVENT_FILE, "r", encoding="utf-8") as f:
            for line in f:
                producer.produce(KAFKA_TOPIC, line.strip(), callback=delivery_report)
                producer.poll(0)
    except FileNotFoundError:
        print(f"Event file '{EVENT_FILE}' not found. Cannot produce events.")
        return
    except OSError as err:
        print(f"Error processing event file '{EVENT_FILE}': {err}")
        return

    producer.flush()
    print("Finished producing events")


if __name__ == "__main__":
    main()
