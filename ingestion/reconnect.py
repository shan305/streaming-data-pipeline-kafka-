import time
from config import (
    RECONNECT_BASE_DELAY,
    RECONNECT_MAX_DELAY,
    RECONNECT_MAX_ATTEMPTS,
)


def reconnect_loop(attempt: int):
    if RECONNECT_MAX_ATTEMPTS and attempt >= RECONNECT_MAX_ATTEMPTS:
        raise RuntimeError(
            "Ingestion halted: reconnect budget exhausted"
        )

    delay = min(RECONNECT_BASE_DELAY * (2 ** attempt), RECONNECT_MAX_DELAY)
    time.sleep(delay)
