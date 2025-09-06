from __future__ import annotations
import math
import threading, time
from queue import Queue, Empty
from typing import Tuple, List
from datetime import datetime
from modbus_monitor.extensions import socketio
from modbus_monitor.database import db as dbsync

class DBWriter(threading.Thread):
    """Ghi điểm đo vào DB theo lô: (tag_id, ts, value)"""
    def __init__(self, q: "Queue[Tuple[int, datetime, float]]",
                 flush_every=0.5, batch_size=200, name="db-writer"):
        super().__init__(name=name, daemon=True)
        self.q = q
        self.buf: List[Tuple[int, datetime, float]] = []
        self.flush_every = flush_every
        self.batch_size = batch_size
        self._stop = threading.Event()

    def run(self):
        last = time.time()
        while not self._stop.is_set():
            try:
                item = self.q.get(timeout=0.2)
                self.buf.append(item)
            except Empty:
                pass

            now = time.time()
            # print("[DBWriter] Log at", now)
            if self.buf and (len(self.buf) >= self.batch_size or (now - last) >= self.flush_every):
                try:
                    cleaned = []
                    data_2_FE = []
                    for tag_id, ts, value in self.buf:
                        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                            value = 0
                        cleaned.append((tag_id, ts, value))
                        data_2_FE.append({
                            "id": tag_id,
                            "name": "tag_test",
                            "value": value,
                            "ts": ts.strftime("%H:%M") if ts else "--:--"
                        })
                    dbsync.insert_tag_values_bulk(cleaned)
                    dbsync.update_device_status_by_tag(tag_id, True)
                    # socketio.emit("update_tags", {"tags": data_2_FE})
                finally:
                    self.buf.clear()
                    last = now

        # flush cuối
        if self.buf:
            dbsync.insert_tag_values_bulk(self.buf)
            self.buf.clear()

    def stop(self):
        self._stop.set()
