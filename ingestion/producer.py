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


def on_message(ws, message):
    data = json.loads(message)

    payload = {
        "symbol": data["s"].lower(),
        "price": data["p"],
        "quantity": data["q"],
        "event_time_ms": data["T"],
    }

    producer.produce(
        topic=KAFKA_TOPIC_TRADES,
        key=f"{payload['symbol']}-{payload['event_time_ms']}",
        value=json.dumps(payload),
    )
    producer.poll(0)
    metrics.inc("messages_produced")


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
