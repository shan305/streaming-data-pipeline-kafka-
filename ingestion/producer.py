"""
Kafka ingestion producer.

Kafka is the FIRST durability boundary.
If Kafka does not acknowledge a message, it is treated as LOST.

Downstream systems rely only on Kafka-acknowledged events.
"""

import json
import websocket
from confluent_kafka import Producer
from config import (
    BINANCE_WS_URL,
    SYMBOLS,
    KAFKA_TOPIC_TRADES,
    KAFKA_PRODUCER_CONFIG,
)
from ingestion.reconnect import reconnect_loop
from observability.metrics import metrics

producer = Producer(KAFKA_PRODUCER_CONFIG)


def delivery_report(err, msg):
    if err:
        metrics.inc("errors")
        raise RuntimeError(f"Kafka produce failed: {err}")


def on_message(ws, message):
    data = json.loads(message)

    payload = {
        "symbol": data["s"].lower(),
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "event_time_ms": data["T"],
    }

    producer.produce(
        topic=KAFKA_TOPIC_TRADES,
        key=f"{payload['symbol']}:{payload['event_time_ms']}",
        value=json.dumps(payload),
        on_delivery=delivery_report,
    )
    producer.poll(0)

    metrics.inc("ingestion.events.produced")
    metrics.record_ingest_lag(payload["symbol"], payload["event_time_ms"])


def start():
    streams = "/".join(f"{s}@trade" for s in SYMBOLS)
    url = f"{BINANCE_WS_URL}/{streams}"

    attempt = 0
    while True:
        try:
            ws = websocket.WebSocketApp(url, on_message=on_message)
            ws.run_forever()
        except Exception:
            attempt += 1
            reconnect_loop(attempt)


if __name__ == "__main__":
    start()
