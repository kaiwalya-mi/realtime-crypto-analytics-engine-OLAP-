# ============================================================
# File 1: binance_producer.py
# Description: Ingests live trade events from Binance WebSocket 
#              and publishes JSON messages to Kafka.
# ============================================================

import json
import logging
import signal
import sys
from websocket import WebSocketApp
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'binance_raw'
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/!miniTicker@arr"

DEDICATED_PARTITIONS = {
    'BTCUSDT': 0,
    'ETHUSDT': 1,
    'SOLUSDT': 2,
    'SUIUSDT': 3,
    'CHIPUSDT': 4,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks=1
    )

producer = create_kafka_producer()

def get_partition(symbol: str) -> int:
    symbol_upper = symbol.upper()
    if symbol_upper in DEDICATED_PARTITIONS:
        return DEDICATED_PARTITIONS[symbol_upper]
    return 5 + (hash(symbol_upper) % 5)

def on_message(ws, message):
    try:
        data = json.loads(message)
        events = data if isinstance(data, list) else [data]
        
        for event in events:
            symbol = event.get('s')
            if not symbol:
                continue
                
            payload = {
                'exchange': 'binance',
                'symbol': symbol,
                'price': float(event.get('c', 0.0)),
                'quantity': float(event.get('q', event.get('v', 0.0))),
                'timestamp': int(event.get('E', 0)),
                'is_buyer_maker': event.get('m', False)
            }
            
            partition_id = get_partition(symbol)
            producer.send(
                topic=KAFKA_TOPIC,
                key=symbol,
                value=payload,
                partition=partition_id
            )
    except Exception as exc:
        logging.error(f"Error processing Binance payload: {exc}")

def on_error(ws, error):
    logging.error(f"Binance WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.warning("Binance WebSocket connection closed.")

def on_open(ws):
    logging.info("Connected to Binance WebSocket live stream.")

def graceful_shutdown(sig, frame):
    logging.info("Shutting down Binance producer...")
    producer.flush()
    producer.close()
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    ws_app = WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws_app.run_forever()
