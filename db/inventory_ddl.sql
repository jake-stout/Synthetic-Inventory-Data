-- Schema definition for inventory simulation
CREATE TABLE part_master (
    part_number VARCHAR PRIMARY KEY,
    part_description TEXT NOT NULL,
    part_condition VARCHAR NOT NULL,
    unit_of_measure VARCHAR NOT NULL,
    standard_cost NUMERIC(10,2),
    manufacturer VARCHAR,
    category VARCHAR
);

CREATE TABLE facilities (
    facility_code VARCHAR PRIMARY KEY,
    facility_name VARCHAR NOT NULL,
    address TEXT,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR
);

CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR UNIQUE NOT NULL,
    role VARCHAR
);

CREATE TABLE vendors (
    vendor_id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    address TEXT,
    contact_email VARCHAR
);

CREATE TABLE work_orders (
    work_order_number VARCHAR PRIMARY KEY,
    site VARCHAR,
    status VARCHAR,
    customer VARCHAR,
    aircraft_type VARCHAR,
    aircraft_tail VARCHAR,
    started TIMESTAMP,
    closed TIMESTAMP,
    posted TIMESTAMP,
    facility_address TEXT
);

CREATE TABLE inventory_transactions (
    transaction_id UUID PRIMARY KEY,
    transaction_type VARCHAR CHECK (transaction_type IN ('RECEIPT','ISSUE','SCRAP','TRANSFER_IN','TRANSFER_OUT','ADJUSTMENT')),
    part_number VARCHAR REFERENCES part_master(part_number),
    lot_number VARCHAR,
    warehouse VARCHAR,
    bin VARCHAR,
    source_facility VARCHAR REFERENCES facilities(facility_code),
    destination_facility VARCHAR REFERENCES facilities(facility_code),
    quantity INTEGER,
    unit_cost NUMERIC(10,2),
    total_cost NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_cost) STORED,
    work_order_number VARCHAR REFERENCES work_orders(work_order_number),
    transaction_timestamp TIMESTAMP,
    created_by INTEGER REFERENCES users(user_id)
);

CREATE TABLE inventory_on_hand (
    part_number VARCHAR REFERENCES part_master(part_number),
    lot_number VARCHAR,
    warehouse VARCHAR,
    bin VARCHAR,
    quantity_on_hand INTEGER,
    last_updated TIMESTAMP,
    PRIMARY KEY (part_number, lot_number, warehouse, bin)
);

-- Table for storing raw streamed transactions from Kafka
CREATE TABLE streamed_transactions (
    transaction_id UUID PRIMARY KEY,
    transaction_type VARCHAR,
    part_number VARCHAR,
    lot_number VARCHAR,
    warehouse VARCHAR,
    bin VARCHAR,
    source_facility VARCHAR,
    destination_facility VARCHAR,
    quantity INTEGER,
    unit_cost NUMERIC(10,2),
    total_cost NUMERIC(12,2),
    work_order_number VARCHAR,
    transaction_timestamp TIMESTAMP,
    created_by INTEGER
);
