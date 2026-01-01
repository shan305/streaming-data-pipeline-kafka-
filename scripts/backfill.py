"""
Manual backfill script.
Replays historical data through the same Kafka topic.
"""
import json
from confluent_kafka import Producer
from config import KAFKA_PRODUCER_CONFIG, KAFKA_TOPIC_TRADES

producer = Producer(KAFKA_PRODUCER_CONFIG)

with open("historical.json") as f:
    for line in f:
        event = json.loads(line)
        producer.produce(
            KAFKA_TOPIC_TRADES,
            key=f"{event['symbol']}-{event['event_time_ms']}",
            value=json.dumps(event),
        )
        producer.poll(0)

producer.flush()
