import os
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "inventory_events")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "inventory")
DB_USER = os.getenv("DB_USER", "inventory")
DB_PASSWORD = os.getenv("DB_PASSWORD", "inventory")


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    source_ddl = f"""
        CREATE TABLE inventory_events (
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
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink_inventory',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """

    sink_ddl = f"""
        CREATE TABLE inventory_on_hand (
            part_number STRING,
            lot_number STRING,
            warehouse STRING,
            bin STRING,
            quantity_on_hand BIGINT,
            last_updated TIMESTAMP(3),
            PRIMARY KEY (part_number, lot_number, warehouse, bin) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}',
            'table-name' = 'inventory_on_hand',
            'username' = '{DB_USER}',
            'password' = '{DB_PASSWORD}'
        )
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    result_sql = """
        INSERT INTO inventory_on_hand
        SELECT
            part_number,
            lot_number,
            warehouse,
            bin,
            SUM(CASE
                    WHEN transaction_type IN ('RECEIPT','TRANSFER_IN','ADJUSTMENT') THEN quantity
                    WHEN transaction_type IN ('ISSUE','SCRAP','TRANSFER_OUT') THEN -quantity
                    ELSE 0
                END) AS quantity_on_hand,
            MAX(transaction_timestamp) AS last_updated
        FROM inventory_events
        GROUP BY part_number, lot_number, warehouse, bin
    """

    table_result = t_env.execute_sql(result_sql)
    table_result.wait()


if __name__ == '__main__':
    main()
