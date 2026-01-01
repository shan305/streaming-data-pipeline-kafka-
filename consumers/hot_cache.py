from consumers.base import BaseConsumer
from storage.redis_client import set_latest_price
from config import KAFKA_TOPIC_TRADES, KAFKA_CONSUMER_GROUP_HOT
from observability.metrics import metrics


class HotCacheConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(KAFKA_CONSUMER_GROUP_HOT, KAFKA_TOPIC_TRADES)

    def process(self, msg: dict):
        symbol = msg["symbol"]
        price = float(msg["price"])
        event_time = msg["event_time_ms"]

        set_latest_price(symbol, price)
        metrics.record_lag(symbol, event_time)

if __name__ == "__main__":
    consumer = HotCacheConsumer()
    consumer.run()
