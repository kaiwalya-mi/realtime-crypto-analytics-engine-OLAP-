Real-Time Cryptocurrency Analytics & Charting Engine

A production-grade, near-real-time streaming analytics platform built to process live market trades from multiple crypto exchanges with 2–5 seconds end-to-end latency. Inspired by trading platforms like TradingView, this pipeline ingests live tick data, calculates windowed market metrics (such as VWAP, standard deviation, and volume metrics), persists records in ClickHouse, and visualizes them on an embedded Grafana dashboard.

Architecture Overview

+------------------+     +---------------+     +---------------+     +--------------------+     +-------------------+
| Binance / MEXC   | --> | Apache Kafka  | --> |  Apache Flink | --> | ClickHouse Database| --> | Grafana / NGINX   |
| (WebSockets)     |     | (Custom Part) |     | (PyFlink/SQL) |     | (OLAP + MVs)       |     | (Embedded UI)     |
+------------------+     +---------------+     +---------------+     +--------------------+     +-------------------+


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

Pipeline Deep Dive

1. Data Ingestion (Kafka Producers)

Multi-Exchange Ingestion: Connects to Binance (JSON) and MEXC (Protobuf) WebSockets to stream trades for 50 cryptocurrencies.

Custom Partitioning Logic: Located in src/kafka_producers/custom_partitioner.py. Decouples high-volume coins into individual partitions (0–4) to prevent partition skew, distributing remaining coins across partitions 5–9.

Multi-Threaded Streams: To abide by MEXC's limit of 30 streams per connection, ingestion is split across parallel producers running on separate threads.

Schema Governance: Schema Registry enforces forward compatibility using Avro, protecting the pipeline against unexpected exchange payload modifications.

src/kafka_producers/
├── DS1/                       # Binance Producer (JSON)
├── DS2/                       # MEXC Producer (Protobuf + Multithreaded)
├── custom_partitioner.py      # Custom routing logic
└── WebSocketProducer.py       # Base class handling reconnects & heartbeat pings


2. Stream Processing (Apache Flink)

Data flows through a Medallion-style architecture managed by a PyFlink orchestrator (execute_all_sql_files.py):

Bronze (Raw Ingestion): Ingests raw JSON/Protobuf feeds directly from Kafka topics.

Silver (Normalized Stream): Strips unused metadata, handles type casting, and standardizes schema structures across all exchanges.

Gold (Derived Metrics): Aggregates candle windows using Flink SQL over four intervals:

1s Tumbling

5s Sliding (1s step)

1m Sliding (5s step)

5m Sliding (30s step)

Custom Java Extensions (/src/flink/UDAFs)

FirstValueAggFunction & LastValueAggFunction: Re-implemented FIRST_VALUE / LAST_VALUE to extract accurate candle Open/Close prices within grouped sliding windows.

StdDevPopAggFunction: Custom Population Standard Deviation function designed to avoid Flink's native NaN-to-DECIMAL conversion crashes during zero-trade windows.

3. Storage & Analytics (ClickHouse OLAP)

ClickHouse acts as the primary analytical serving layer, organizing data into a three-step processing pipeline:

[Kafka Engine Table]  -->  [Materialized View Trigger]  -->  [MergeTree Storage Engine]
