-- Flink SQL job to read transactions from Kafka and write to PostgreSQL

CREATE TABLE source_transactions (
    transaction_id STRING,
    transaction_type STRING,
    part_number STRING,
    lot_number STRING,
    warehouse STRING,
    bin STRING,
    source_facility STRING,
    destination_facility STRING,
    quantity INT,
    unit_cost DOUBLE,
    total_cost DOUBLE,
    work_order_number STRING,
    transaction_timestamp TIMESTAMP(3),
    created_by INT,
    WATERMARK FOR transaction_timestamp AS transaction_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory_transactions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE streamed_transactions (
    transaction_id STRING,
    transaction_type STRING,
    part_number STRING,
    lot_number STRING,
    warehouse STRING,
    bin STRING,
    source_facility STRING,
    destination_facility STRING,
    quantity INT,
    unit_cost DOUBLE,
    total_cost DOUBLE,
    work_order_number STRING,
    transaction_timestamp TIMESTAMP(3),
    created_by INT,
    PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/inventory',
    'table-name' = 'streamed_transactions',
    'username' = 'inventory',
    'password' = 'inventory'
);

INSERT INTO streamed_transactions
SELECT * FROM source_transactions;
