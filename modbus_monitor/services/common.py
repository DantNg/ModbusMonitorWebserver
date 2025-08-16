from __future__ import annotations
import threading
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional

def utc_now():
    return datetime.now(timezone.utc)

class LatestCache:
    """Thread-safe cache: tag_id -> (ts, value)"""
    def __init__(self):
        self._lock = threading.RLock()
        self._data: Dict[int, Tuple[datetime, float]] = {}

    def set(self, tag_id: int, ts: datetime, value: float):
        with self._lock:
            self._data[tag_id] = (ts, value)

    def get(self, tag_id: int) -> Optional[Tuple[datetime, float]]:
        with self._lock:
            return self._data.get(tag_id)

    def get_many(self, tag_ids):
        with self._lock:
            return {tid: self._data.get(tid) for tid in tag_ids}
