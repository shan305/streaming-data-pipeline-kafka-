"""
Redis client for hot-path latest prices.
"""
import redis
from config import REDIS_HOST, REDIS_PORT, REDIS_TTL_SECONDS

_redis = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)


def set_latest_price(symbol: str, price: float):
    key = f"price:{symbol}"
    _redis.set(key, price, ex=REDIS_TTL_SECONDS)


def get_latest_price(symbol: str):
    return _redis.get(f"price:{symbol}")
