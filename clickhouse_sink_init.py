# ============================================================
# File 4: clickhouse_sink_init.py
# Description: Initializes ClickHouse database tables, Kafka engine
#              connectors, and Materialized Views for metrics storage.
# ============================================================

import logging
from clickhouse_driver import Client

CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
DATABASE_NAME = 'crypto_analytics'

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def init_clickhouse_schema():
    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    
    client.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    client.execute(f"USE {DATABASE_NAME}")
    
    logging.info("Creating Target MergeTree Table...")
    client.execute("""
        CREATE TABLE IF NOT EXISTS market_metrics_gold (
            symbol String,
            window_start DateTime,
            window_end DateTime,
            open_price Float64,
            high_price Float64,
            low_price Float64,
            close_price Float64,
            volume Float64,
            vwap Float64,
            trade_count UInt64,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (symbol, window_start)
        PRIMARY KEY (symbol, window_start);
    """)

    logging.info("Creating Kafka Queue Engine Connector...")
    client.execute("""
        CREATE TABLE IF NOT EXISTS kafka_gold_stream (
            symbol String,
            window_start String,
            window_end String,
            open_price Float64,
            high_price Float64,
            low_price Float64,
            close_price Float64,
            volume Float64,
            vwap Float64,
            trade_count UInt64
        ) ENGINE = Kafka
        SETTINGS kafka_broker_list = 'localhost:9092',
                 kafka_topic_list = 'market_metrics_gold',
                 kafka_group_name = 'clickhouse_metrics_consumer',
                 kafka_format = 'JSONEachRow';
    """)

    logging.info("Creating Materialized View...")
    client.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_gold_metrics
        TO market_metrics_gold AS
        SELECT
            symbol,
            parseDateTimeBestEffort(window_start) AS window_start,
            parseDateTimeBestEffort(window_end) AS window_end,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            vwap,
            trade_count
        FROM kafka_gold_stream;
    """)

    logging.info("ClickHouse schema setup completed successfully.")

if __name__ == "__main__":
    init_clickhouse_schema()
