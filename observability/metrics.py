"""
Lightweight observability.

Tracks:
- Semantic counters
- Ingestion lag
- Throughput
- Health signals

No external dependencies by design.
"""
import time
import threading
import logging
from collections import defaultdict
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class Metrics:
    """
    Thread-safe metrics collector.
    
    Tracks:
    - Counters (messages received, produced, errors)
    - Lag per symbol (event time vs processing time)
    - Throughput (messages per second)
    """
    
    def __init__(self):
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = defaultdict(int)
        self._lag_ms: Dict[str, float] = {}
        self._last_snapshot_time = time.time()
        self._last_snapshot_counts: Dict[str, int] = {}
    
    def inc(self, name: str, value: int = 1) -> None:
        """Increment a counter."""
        with self._lock:
            self._counters[name] += value
    
    def record_lag(self, symbol: str, event_time_ms: int) -> None:
        """
        Record processing lag for a symbol.
        event_time_ms: timestamp from the exchange (milliseconds)
        """
        now_ms = time.time() * 1000
        lag = now_ms - event_time_ms
        
        with self._lock:
            self._lag_ms[symbol] = lag
        
        # Warn if lag is high
        from config import LAG_WARNING_THRESHOLD_MS
        if lag > LAG_WARNING_THRESHOLD_MS:
            logger.warning(f"High lag for {symbol}: {lag:.0f}ms")
    
    def get_counter(self, name: str) -> int:
        """Get current counter value."""
        with self._lock:
            return self._counters[name]
    
    def snapshot(self) -> dict:
        """
        Get current metrics snapshot.
        Includes throughput calculation.
        """
        with self._lock:
            now = time.time()
            elapsed = now - self._last_snapshot_time
            
            # Calculate throughput
            throughput = {}
            for name, count in self._counters.items():
                prev = self._last_snapshot_counts.get(name, 0)
                if elapsed > 0:
                    throughput[f"{name}_per_sec"] = (count - prev) / elapsed
            
            # Update snapshot baseline
            self._last_snapshot_time = now
            self._last_snapshot_counts = dict(self._counters)
            
            return {
                "counters": dict(self._counters),
                "lag_ms": dict(self._lag_ms),
                "throughput": throughput,
                "timestamp": now,
            }
    
    def health_check(self) -> dict:
        """
        Simple health check.
        Returns status and any issues detected.
        """
        issues = []
        
        with self._lock:
            # Check for high lag
            from config import LAG_WARNING_THRESHOLD_MS
            for symbol, lag in self._lag_ms.items():
                if lag > LAG_WARNING_THRESHOLD_MS:
                    issues.append(f"High lag on {symbol}: {lag:.0f}ms")
            
            # Check for errors
            error_count = self._counters.get("errors", 0)
            if error_count > 0:
                issues.append(f"Errors recorded: {error_count}")
        
        return {
            "status": "unhealthy" if issues else "healthy",
            "issues": issues,
        }


# Global metrics instance
metrics = Metrics()
