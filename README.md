# Synthetic-Inventory-Data

This project demonstrates a small end-to-end pipeline for working with
synthetic inventory transactions.  Data is generated and loaded into a
PostgreSQL database, events are published to Kafka, and Apache Flink
maintains an inventory-on-hand table by consuming those events.

## Prerequisites

- Docker and docker-compose
- Or a local Python 3 environment with the packages from
  `requirements.txt`

## Quick start with Docker

1. Start the infrastructure:
   ```bash
   docker-compose up -d
   ```
   This launches PostgreSQL, Kafka, Zookeeper, and the Flink services.

   PGAdmin is also available at [http://localhost:5052](http://localhost:5052)
   with the default email `admin@admin.com` and password `admin`.

2. Generate sample data and load it into PostgreSQL:
   ```bash
   docker-compose run --rm producer python scripts/generate_synthetic_data.py
   ```

3. Produce inventory events to Kafka:
   ```bash
   docker-compose run --rm producer python scripts/produce_inventory_events.py
   ```

4. Process the events with Flink:
   ```bash
   docker-compose run --rm flink-app python scripts/flink_inventory_processor.py
   ```

## Running locally

If you prefer to run the scripts directly, install the dependencies and
ensure Postgres and Kafka are available:

```bash
pip install -r requirements.txt
python scripts/generate_synthetic_data.py
python scripts/produce_inventory_events.py
python scripts/flink_inventory_processor.py
```

Environment variables such as database connection info or Kafka
configuration can be overridden in a `.env` file or through the shell.

## Project structure

- `db/` – PostgreSQL DDL used when initializing the database
- `scripts/` – Utilities for generating data, producing Kafka events and
  running the Flink job
- `docker/` – Dockerfiles for the producer and Flink application
- `docker-compose.yml` – Orchestrates the containerized environment

