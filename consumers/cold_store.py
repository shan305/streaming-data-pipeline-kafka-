from sqlalchemy.exc import IntegrityError
from consumers.base import BaseConsumer
from storage.models import Trade, SessionLocal, init_db
from config import KAFKA_TOPIC_TRADES, KAFKA_CONSUMER_GROUP_COLD

init_db()


class ColdStoreConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(KAFKA_CONSUMER_GROUP_COLD, KAFKA_TOPIC_TRADES)

    def process(self, msg: dict):
        db = SessionLocal()
        try:
            trade = Trade(
                symbol=msg["symbol"],
                price=msg["price"],
                quantity=msg["quantity"],
                event_time_ms=msg["event_time_ms"],
            )
            db.add(trade)
            db.commit()
        except IntegrityError:
            db.rollback()  # duplicate → safe ignore
        finally:
            db.close()


if __name__ == "__main__":
    consumer = ColdStoreConsumer()
    consumer.run()