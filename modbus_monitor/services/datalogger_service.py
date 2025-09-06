from __future__ import annotations
import threading, time
from typing import Dict, List, Tuple

import socketio
from modbus_monitor.database import db as dbsync
from modbus_monitor.services.common import LatestCache, utc_now

class DataLoggerService(threading.Thread):
    """Theo lịch từng logger → snapshot cache → ghi bulk."""
    def __init__(self, cache: LatestCache, tick_sec: float = 0.5):
        super().__init__(name="datalogger-service", daemon=True)
        self.cache = cache
        self.tick = tick_sec
        self._stop = threading.Event()
        # scheduler rất đơn giản: logger_id -> next_due
        self._next_due: Dict[int, float] = {}

    def _ensure_loggers(self):
        now = time.time()
        for l in (dbsync.list_data_loggers() or []):
            if not l.get("enabled", True): continue
            lid = int(l["id"])
            if lid not in self._next_due:
                self._next_due[lid] = now + 0.1

    def run(self):
        while not self._stop.is_set():
            try:
                self._ensure_loggers()
                now = time.time()
                for l in (dbsync.list_data_loggers() or []):
                    if not l.get("enabled", True): continue
                    lid = int(l["id"])
                    itv = max(0.1, float(l.get("interval_sec") or 60))
                    due = self._next_due.get(lid, now + 0.1)
                    if now >= due:
                        tag_ids = dbsync.list_data_logger_tags(lid) or []
                        rows = []
                        ts = utc_now()
                        if tag_ids:
                            kv = self.cache.get_many(tag_ids)
                            for tid, rec in kv.items():
                                if rec:
                                    _, val = rec
                                    rows.append((int(tid), ts, float(val)))
                        if rows:
                            dbsync.insert_tag_values_bulk(rows)
                            print("update_tags")
                        self._next_due[lid] = now + itv
            except Exception:
                pass
            time.sleep(self.tick)

    def stop(self):
        self._stop.set()
