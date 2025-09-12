from __future__ import annotations
import threading, time
from typing import Dict, List, Tuple

import socketio
from modbus_monitor.database import db as dbsync
from modbus_monitor.services.common import LatestCache, utc_now

class DataLoggerService(threading.Thread):
    """Simple precise timing scheduler với anti-drift logic."""
    def __init__(self, cache: LatestCache):
        super().__init__(name="datalogger-service", daemon=True)
        self.cache = cache
        self._stop = threading.Event()
        
        # Per-logger scheduling (anti-drift): logger_id -> next_run_time
        self._next_runs: Dict[int, float] = {}
        # Track intervals: logger_id -> interval_sec
        self._intervals: Dict[int, float] = {}

    def _execute_logger(self, logger_config: dict):
        """Execute logging for a single logger"""
        lid = int(logger_config["id"])
        logger_name = logger_config.get("name", f"Logger_{lid}")
        
        try:
            tag_ids = dbsync.list_data_logger_tags(lid) or []
            rows = []
            ts = utc_now().astimezone().replace(tzinfo=None)
            
            if tag_ids:
                kv = self.cache.get_many(tag_ids)
                for tid, rec in kv.items():
                    if rec:
                        _, val = rec
                        rows.append((int(tid), ts, float(val)))
            
            if rows:
                dbsync.insert_tag_values_bulk(rows)
                print(f"✅ {logger_name}: Logged at {ts.isoformat()}")
            else:
                print(f"📝 {logger_name}: No data to log")
                
        except Exception as e:
            print(f"❌ {logger_name}: Error - {e}")

    def run(self):
        """Main loop with anti-drift timing"""
        while not self._stop.is_set():
            try:
                now = time.monotonic()
                
                for logger in (dbsync.list_data_loggers() or []):
                    if not logger.get("enabled", True):
                        continue
                        
                    lid = int(logger["id"])
                    interval = max(0.1, float(logger.get("interval_sec") or 60))
                    # print(f"⏱️ Checking Logger {lid} (interval={interval}s)")
                    # Check if interval changed
                    if self._intervals.get(lid) != interval:
                        print(f"🔄 Logger {lid}: Interval changed to {interval}s")
                        self._intervals[lid] = interval
                        self._next_runs[lid] = now + 0.1  # Run soon for immediate effect
                        continue
                    
                    # Initialize if new logger
                    if lid not in self._next_runs:
                        print(f"🆕 Logger {lid}: First run - interval {interval}s")
                        self._intervals[lid] = interval
                        self._next_runs[lid] = now + 0.1  # Run soon for immediate effect
                        continue
                    
                    # Check if due to run
                    next_run = self._next_runs[lid]
                    if now >= next_run:  # Avoid huge catch-up runs
                        # print(f"🚀 Logger {lid}: Executing (interval={interval}s)")
                        
                        # Execute logging
                        self._execute_logger(logger)
                        
                        # Schedule next run (anti-drift)
                        self._next_runs[lid] = next_run + interval
                        
                        # If we're behind, catch up gradually (but don't spam)
                        max_catchup = 3  # Maximum 3 intervals to catch up
                        catchup_count = 0
                        while self._next_runs[lid] < now and catchup_count < max_catchup:
                            self._next_runs[lid] += interval
                            catchup_count += 1
                
            except Exception as e:
                print(f"❌ DataLogger main loop error: {e}")
                
            self._stop.wait(0.1)
    
    def stop(self):
        """Stop the datalogger service"""
        print("🛑 Stopping DataLogger service...")
        self._stop.set()
