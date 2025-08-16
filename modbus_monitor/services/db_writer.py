from __future__ import annotations
import threading, time
from queue import Queue, Empty
from typing import Tuple, List
from datetime import datetime
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
            if self.buf and (len(self.buf) >= self.batch_size or (now - last) >= self.flush_every):
                try:
                    dbsync.insert_tag_values_bulk(self.buf)
                finally:
                    self.buf.clear()
                    last = now

        # flush cuối
        if self.buf:
            dbsync.insert_tag_values_bulk(self.buf)
            self.buf.clear()

    def stop(self):
        self._stop.set()
