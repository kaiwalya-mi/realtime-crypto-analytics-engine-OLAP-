# ============================================================
# File 2: mexc_producer.py
# Description: Consumes live trade feeds from MEXC WebSocket,
#              serializes into Protobuf/JSON, and writes to Kafka.
# ============================================================

import json
import logging
import signal
import sys
import time
import websocket
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'mexc_raw'
MEXC_WS_URL = "wss://wbs.mexc.com/ws"

DEDICATED_PARTITIONS = {
    'BTCUSDT': 0,
    'ETHUSDT': 1,
    'SOLUSDT': 2,
    'SUIUSDT': 3,
    'CHIPUSDT': 4,
}

SUBSCRIBE_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT", "CHIPUSDT"]

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
        data = json.loads(message) if isinstance(message, str) else json.loads(message.decode('utf-8'))
        
        if data.get("c") == "spot@public.deals.v3.api@":
            symbol = data.get("s")
            deals = data.get("d", {}).get("deals", [])
            
            for deal in deals:
                payload = {
                    'exchange': 'mexc',
                    'symbol': symbol,
                    'price': float(deal.get('p', 0.0)),
                    'quantity': float(deal.get('v', 0.0)),
                    'timestamp': int(deal.get('t', time.time() * 1000)),
                    'trade_type': int(deal.get('S', 1))
                }
                
                partition_id = get_partition(symbol)
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=symbol,
                    value=payload,
                    partition=partition_id
                )
    except Exception as exc:
        logging.error(f"Error parsing MEXC message: {exc}")

def on_error(ws, error):
    logging.error(f"MEXC WebSocket error: {error}")

def on_close(ws, status, msg):
    logging.warning("MEXC WebSocket stream closed.")

def on_open(ws):
    logging.info("Connected to MEXC WebSocket.")
    params = [f"spot@public.deals.v3.api@{symbol}" for symbol in SUBSCRIBE_PAIRS]
    sub_payload = {
        "method": "SUBSCRIPTION",
        "params": params
    }
    ws.send(json.dumps(sub_payload))

def graceful_shutdown(sig, frame):
    logging.info("Terminating MEXC Producer...")
    producer.flush()
    producer.close()
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    ws_app = websocket.WebSocketApp(
        MEXC_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws_app.run_forever()
