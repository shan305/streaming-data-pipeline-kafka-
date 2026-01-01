"""
Hot-path cache for latest prices.

This store is explicitly NON-AUTHORITATIVE:
- Data may be dropped
- Overwrites are expected
- TTL enforced
- No replay or recovery guarantees

Correctness of the system MUST NOT depend on Redis.
"""

import redis
from config import REDIS_HOST, REDIS_PORT, REDIS_TTL_SECONDS

KEY_PREFIX = "v1:price"

_redis = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)


def set_latest_price(symbol: str, price: float) -> None:
    """
    Best-effort write of latest price.
    Loss is acceptable by design.
    """
    key = f"{KEY_PREFIX}:{symbol}"
    _redis.set(key, price, ex=REDIS_TTL_SECONDS)


def get_latest_price(symbol: str):
    """
    Best-effort read.
    May return None if expired or missing.
    """
    return _redis.get(f"{KEY_PREFIX}:{symbol}")
