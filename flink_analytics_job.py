# ============================================================
# File 3: flink_analytics_job.py
# Description: Core PyFlink streaming job reading raw Kafka feeds,
#              normalizing into Silver tier, and producing Gold 
#              OHLC/VWAP time-window metrics.
# ============================================================

import os
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def build_flink_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(1000, CheckpointingMode.AT_LEAST_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(500)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", 
        "file://" + os.path.abspath("flink-sql-connector-kafka_2.12-1.17.1.jar")
    )

    t_env.execute_sql("""
        CREATE TABLE binance_raw_source (
            exchange STRING,
            symbol STRING,
            price DOUBLE,
            quantity DOUBLE,
            `timestamp` BIGINT,
            is_buyer_maker BOOLEAN,
            event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)),
            WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'binance_raw',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink_binance_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE mexc_raw_source (
            exchange STRING,
            symbol STRING,
            price DOUBLE,
            quantity DOUBLE,
            `timestamp` BIGINT,
            trade_type INT,
            event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)),
            WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'mexc_raw',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink_mexc_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE kafka_gold_metrics_sink (
            symbol STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            volume DOUBLE,
            vwap DOUBLE,
            trade_count BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'market_metrics_gold',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE VIEW silver_unified_trades AS
        SELECT exchange, symbol, price, quantity, event_time FROM binance_raw_source
        UNION ALL
        SELECT exchange, symbol, price, quantity, event_time FROM mexc_raw_source
    """)

    t_env.execute_sql("""
        INSERT INTO kafka_gold_metrics_sink
        SELECT 
            symbol,
            TUMBLE_START(event_time, INTERVAL '5' SECOND) AS window_start,
            TUMBLE_END(event_time, INTERVAL '5' SECOND) AS window_end,
            FIRST_VALUE(price) AS open_price,
            MAX(price) AS high_price,
            MIN(price) AS low_price,
            LAST_VALUE(price) AS close_price,
            SUM(quantity) AS volume,
            SUM(price * quantity) / NULLIF(SUM(quantity), 0) AS vwap,
            COUNT(*) AS trade_count
        FROM silver_unified_trades
        GROUP BY 
            symbol, 
            TUMBLE(event_time, INTERVAL '5' SECOND)
    """)

if __name__ == "__main__":
    build_flink_pipeline()
