"""
Base Kafka consumer with manual offset control.
"""
import json
import logging
from confluent_kafka import Consumer
from config import KAFKA_CONSUMER_CONFIG
from observability.metrics import metrics

logger = logging.getLogger(__name__)


class BaseConsumer:
    def __init__(self, group_id: str, topic: str):
        config = dict(KAFKA_CONSUMER_CONFIG)
        config["group.id"] = group_id

        self.consumer = Consumer(config)
        self.consumer.subscribe([topic])

    def process(self, message: dict):
        raise NotImplementedError

    def run(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                metrics.inc("errors")
                logger.error(msg.error())
                continue

            try:
                payload = json.loads(msg.value())
                self.process(payload)
                self.consumer.commit(msg)
                metrics.inc("messages_consumed")
            except Exception as e:
                metrics.inc("errors")
                logger.exception("Consumer failure", exc_info=e)
