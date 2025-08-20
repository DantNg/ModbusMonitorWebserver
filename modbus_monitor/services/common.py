"""
Module providing a thread-safe cache for storing the latest timestamped values associated with tag IDs.
Classes:
    LatestCache: 
        A thread-safe cache for storing and retrieving the latest value and timestamp for each tag ID.
        - Uses a reentrant lock (RLock) to ensure safe concurrent access.
        - Stores data as a dictionary mapping tag IDs (int) to a tuple of (timestamp, value).
        - Provides methods to set a value, get a value for a single tag ID, and get values for multiple tag IDs.
Functions:
    utc_now():
        Returns the current UTC datetime with timezone information.
"""
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
