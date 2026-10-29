Real-Time Cryptocurrency Analytics & Charting Engine

A production-grade, near-real-time streaming analytics platform built to process live market trades from multiple crypto exchanges with 2–5 seconds end-to-end latency. Inspired by trading platforms like TradingView, this system ingests raw tick data, computes rolling windowed technical indicators (VWAP, standard deviation, volume metrics), persists time-series data in ClickHouse, and serves real-time charts on an embedded Grafana frontend.

Technical Architecture & System Flow

The pipeline follows an event-driven streaming architecture designed for decoupled scaling, stateful stream processing, and ultra-low query latency:

+---------------------------------------------------------------------------------------------------------+
|                                             INGESTION LAYER                                             |
|                                                                                                         |
|   +--------------------------+          +-------------------------+                                     |
|   |  Binance WebSocket API   |          |    MEXC WebSocket API   |                                     |
|   |      (JSON Feeds)        |          |   (Protobuf / 30-limit) |                                     |
|   +------------+-------------+          +------------+------------+                                     |
|                |                                     |                                                  |
|                +------------------+------------------+                                                  |
|                                   |                                                                     |
|                                   v                                                                     |
|                     +---------------------------+                                                       |
|                     | Custom Partitioning Engine|  (Hot coins -> Partitions 0-4)                         |
|                     | (WebSocketProducer Class) |  (Hash/modulo -> Partitions 5-9)                        |
|                     +-------------+-------------+                                                       |
+-----------------------------------|---------------------------------------------------------------------+
                                    |
                                    v
+---------------------------------------------------------------------------------------------------------+
|                                         STREAM BROKER LAYER                                             |
|                                                                                                         |
|   +-------------------------------------------------------------------------------------------------+   |
|   |                                         Apache Kafka                                            |   |
|   |    [Topic: binance_raw] (10 Partitions)         |        [Topic: mexc_raw] (10 Partitions)       |   |
|   +------------------------------------------------+------------------------------------------------+   |
|                                                    |                                                    |
|                                    +---------------+---------------+                                    |
|                                    | Avro Schema Registry (Confluent)|                                   |
|                                    +---------------+---------------+                                    |
+----------------------------------------------------|----------------------------------------------------+
                                                     |
                                                     v
+---------------------------------------------------------------------------------------------------------+
|                                        STREAM PROCESSING LAYER                                          |
|                                                                                                         |
|   +-------------------------------------------------------------------------------------------------+   |
|   |                                         Apache Flink                                            |   |
|   |                                                                                                 |   |
|   |  Bronze (Raw Source)  -->  Silver (Normalized Stream)  -->  Gold (Derived Windowed Metrics)       |   |
|   |                                                                                                 |   |
|   |  - Custom Java UDAFs: BoundaryValueAggFunction (OHLC), StdDevPopAggFunction (Zero-Variance)     |   |
|   |  - Checkpointing: 1,000ms At-Least-Once backed by persistent Docker volumes                     |   |
|   +------------------------------------------------+------------------------------------------------+   |
+----------------------------------------------------|----------------------------------------------------+
                                                     | (Upsert Kafka Connector)
                                                     v
+---------------------------------------------------------------------------------------------------------+
|                                         STORAGE & OLAP LAYER                                            |
|                                                                                                         |
|   +-------------------------------------------------------------------------------------------------+   |
|   |                                       ClickHouse Database                                       |   |
|   |                                                                                                 |   |
|   |   [Kafka Engine Tables]  -->  (Materialized Views)  -->  [MergeTree Storage Engine]             |   |
|   |                                                                    |                            |   |
|   |                                                                    v                            |   |
|   |                                                         [Analytical Rollup Views]               |   |
|   +--------------------------------------------------------------------+----------------------------+   |
+------------------------------------------------------------------------|--------------------------------+
                                                                         |
                                                                         v
+---------------------------------------------------------------------------------------------------------+
|                                      FRONTEND & CACHING LAYER                                           |
|                                                                                                         |
|   +-------------------------------------------------------------------------------------------------+   |
|   |  NGINX Reverse Proxy (Global Rate-Limiting + 2s POST Body Caching for ClickHouse Queries)       |   |
|   +------------------------------------------------+------------------------------------------------+   |
|                                                    |                                                    |
|                                                    v                                                    |
|   +-------------------------------------------------------------------------------------------------+   |
|   |                             Embedded Grafana Charts in Vanilla JS UI                            |   |
|   +-------------------------------------------------------------------------------------------------+   |
+---------------------------------------------------------------------------------------------------------+


Tech Stack

Ingestion: Python, WebSockets, Apache Kafka, Avro Schema Registry, Protobuf

Stream Processing: Apache Flink (PyFlink, Flink SQL, Java UDAFs)

Storage & OLAP: ClickHouse (Kafka Engine, Materialized Views, MergeTree)

Visualization & Frontend: Grafana, Vanilla JS, HTML5/CSS3

Infrastructure & Proxying: Docker, NGINX (Reverse Proxy & Edge Caching), Oracle Cloud VM

Key Features & Highlights

Sub-5-Second Latency: Ingests live trades, calculates rolling technical metrics, and renders live candles with minimal delay.

Custom Stream Partitioning: High-volume pairs (BTC, ETH, SOL, SUI, CHIP) are assigned dedicated Kafka partitions, while lower-volume pairs are balanced via custom hash/modulo routing.

Robust Failover & State Recovery: Utilizes Flink’s 1-second at-least-once checkpointing backed by persistent Docker volumes to survive container rebuilds without losing pipeline state.

Smart Data Normalization: Implements stream-level 3NF principles—calculating non-derivable metrics in Flink while deferring composite metrics (e.g., net order flow) to downstream ClickHouse views.

Enterprise Edge Defense: Uses NGINX response caching (2-second TTL) and rate limiting to prevent database exhaustion under high concurrent web traffic.

Metrics Measured & Mathematical Formulations

The pipeline computes granular trading metrics across multiple tumbling and sliding window intervals (1s, 5s, 30s, 1m, 5m, 30m, 1h, 4h, 24h):

1
