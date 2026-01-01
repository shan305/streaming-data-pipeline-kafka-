"""
Crypto Streaming Pipeline - Configuration
All settings centralized. No magic strings.
"""
import os

# =============================================================================
# KAFKA
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TRADES = "crypto.trades.raw"
KAFKA_CONSUMER_GROUP_HOT = "hot-cache-consumer"
KAFKA_CONSUMER_GROUP_COLD = "cold-store-consumer"

# Producer settings
KAFKA_PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "1",  # Leader ack (balance of speed vs durability)
    "linger.ms": 5,  # Batch for 5ms before sending
    "compression.type": "snappy",
}

# Consumer settings
KAFKA_CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # Manual commits for reliability
}

# =============================================================================
# REDIS (Hot Path - Latest Prices)
# =============================================================================
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_TTL_SECONDS = 300  # 5 min retention for hot data

# =============================================================================
# POSTGRES (Cold Path - Historical)
# =============================================================================
POSTGRES_URL = "postgresql://postgres:postgres@localhost:5434/crypto_pipeline"


# =============================================================================
# BINANCE WEBSOCKET
# =============================================================================
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
SYMBOLS = ["btcusdt", "ethusdt", "ltcusdt"]

# =============================================================================
# RECONNECTION SETTINGS
# =============================================================================
RECONNECT_BASE_DELAY = 1  # seconds
RECONNECT_MAX_DELAY = 60  # seconds
RECONNECT_MAX_ATTEMPTS = 0  # 0 = infinite

# =============================================================================
# OBSERVABILITY
# =============================================================================
METRICS_LOG_INTERVAL = 10  # Log metrics every N seconds
LAG_WARNING_THRESHOLD_MS = 5000  # Warn if lag exceeds this
