"""
Manual backfill / replay tool.

Replays historical events through Kafka using the SAME topic,
keying strategy, and schema as live ingestion.

Guarantees:
- Deterministic replay
- Idempotent consumption
- Cold-store rebuild safety

Kafka is the first durability boundary.
"""

import json
from confluent_kafka import Producer
from config import KAFKA_PRODUCER_CONFIG, KAFKA_TOPIC_TRADES

producer = Producer(KAFKA_PRODUCER_CONFIG)


def delivery_report(err, msg):
    if err:
        raise RuntimeError(f"Kafka delivery failed: {err}")


with open("historical.json") as f:
    for line in f:
        event = json.loads(line)

        producer.produce(
            topic=KAFKA_TOPIC_TRADES,
            key=f"{event['symbol']}:{event['event_time_ms']}",
            value=json.dumps(event),
            on_delivery=delivery_report,
        )
        producer.poll(0)

producer.flush()
