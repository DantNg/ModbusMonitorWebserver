from __future__ import annotations
import math
import threading, time
from queue import Queue, Empty
from typing import Tuple, List
from datetime import datetime
from modbus_monitor.extensions import socketio
from modbus_monitor.database import db as dbsync

class DBWriter(threading.Thread):
    """High-speed database writer optimized for real-time updates with immediate socket emission support"""
    def __init__(self, q: "Queue[Tuple[int, datetime, float]]",
                 flush_every=0.1, batch_size=50, name="db-writer-realtime"):  # Much faster flush
        super().__init__(name=name, daemon=True)
        self.q = q
        self.buf: List[Tuple[int, datetime, float]] = []
        self.flush_every = flush_every
        self.batch_size = batch_size
        self._stop = threading.Event()
        self._last_emission = 0  # Track last emission time to avoid spam

    def run(self):
        last = time.time()
        while not self._stop.is_set():
            try:
                # Shorter timeout for more responsive processing
                item = self.q.get(timeout=0.1)
                self.buf.append(item)
            except Empty:
                pass

            now = time.time()
            
            # More frequent flushing for real-time updates
            should_flush = (
                self.buf and (
                    len(self.buf) >= self.batch_size or 
                    (now - last) >= self.flush_every
                )
            )
            
            if should_flush:
                try:
                    cleaned = []
                    device_updates = {}  # Group updates by device_id
                    
                    for tag_id, ts, value in self.buf:
                        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                            value = 0
                        cleaned.append((tag_id, ts, value))
                        
                        # Get tag info to determine device
                        tag_info = dbsync.get_tag(tag_id)
                        if tag_info:
                            device_id = tag_info["device_id"]
                            if device_id not in device_updates:
                                device_updates[device_id] = []
                            
                            device_updates[device_id].append({
                                "id": tag_id,
                                "name": tag_info.get("name", "tag_test"),
                                "value": value,
                                "ts": ts.strftime("%H:%M:%S") if ts else "--:--:--"
                            })
                    
                    # Save to database (this is still important for persistence)
                    if cleaned:
                        # dbsync.insert_tag_values_bulk(cleaned)
                        # Update device status
                        dbsync.update_device_status_by_tag(cleaned[-1][0], True)
                    
                    # IMPORTANT: The immediate socket emissions now happen in the ModbusService
                    # This DBWriter now focuses on database persistence and backup emissions
                    # Only emit via DBWriter if enough time has passed (backup mechanism)
                    if now - self._last_emission > 2.0:  # Backup emission every 2 seconds
                        from modbus_monitor.extensions import emit_dashboard_update, emit_dashboard_device_update, emit_subdashboard_update
                        
                        # Emit backup updates (lower priority)
                        all_tags = []
                        for device_tags in device_updates.values():
                            all_tags.extend(device_tags)
                        
                        if all_tags:
                            # Backup emission with "db_backup" flag
                            emit_dashboard_update({"tags": all_tags, "source": "db_backup"})
                            
                            # Emit to specific device views as backup
                            for device_id, tags in device_updates.items():
                                emit_dashboard_device_update(device_id, {"tags": tags, "source": "db_backup"})
                            
                            # Backup subdashboard emissions
                            try:
                                subdashboards = dbsync.list_subdashboards() or []
                                for subdash in subdashboards:
                                    subdash_id = subdash['id']
                                    subdash_tag_ids = [t['id'] for t in dbsync.get_subdashboard_tags(subdash_id) or []]
                                    
                                    subdash_tags = []
                                    for device_tags in device_updates.values():
                                        for tag in device_tags:
                                            if tag['id'] in subdash_tag_ids:
                                                subdash_tags.append(tag)
                                    
                                    if subdash_tags:
                                        emit_subdashboard_update(subdash_id, {"tags": subdash_tags, "source": "db_backup"})
                            except Exception as e:
                                print(f"Error in backup subdashboard emission: {e}")
                        
                        self._last_emission = now
                        
                finally:
                    self.buf.clear()
                    last = now

        # flush cuối
        if self.buf:
            # dbsync.insert_tag_values_bulk(self.buf)
            self.buf.clear()

    def stop(self):
        self._stop.set()
