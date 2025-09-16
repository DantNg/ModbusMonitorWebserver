from __future__ import annotations
import threading, time
from typing import Dict, List, Tuple
import logging

import socketio
from modbus_monitor.database import db as dbsync
from modbus_monitor.services.common import LatestCache, utc_now
from modbus_monitor.services.value_queue_service import (
    ValueQueueService, RawModbusValue, value_queue_service
)

logger = logging.getLogger(__name__)

class DataLoggerService(threading.Thread):
    """
    DataLogger service đọc từ value queue và ghi DB
    Consumer từ value queue với precise timing scheduler
    """
    def __init__(self, cache: LatestCache):
        super().__init__(name="datalogger-service", daemon=True)
        self.cache = cache
        self._stop = threading.Event()
        
        # Per-logger scheduling (anti-drift): logger_id -> next_run_time
        self._next_runs: Dict[int, float] = {}
        # Track intervals: logger_id -> interval_sec
        self._intervals: Dict[int, float] = {}
        
        # Value buffer for logging (tag_id -> latest_value)
        self._value_buffer: Dict[int, Tuple[float, float]] = {}  # tag_id -> (timestamp, value)
        self._buffer_lock = threading.Lock()
        
        # Queue consumer thread
        self._queue_consumer_thread = None
        
        # Stats
        self.stats = {
            'values_consumed': 0,
            'values_logged': 0,
            'log_executions': 0,
            'start_time': time.time()
        }
        self.stats_lock = threading.Lock()
        
        logger.info("DataLoggerService initialized with queue consumer")

    def start(self):
        """Start both queue consumer and logger scheduler"""
        # Start queue consumer thread
        self._queue_consumer_thread = threading.Thread(
            target=self._queue_consumer_loop,
            daemon=True,
            name="DataLogger-Consumer"
        )
        self._queue_consumer_thread.start()
        
        # Start main thread (scheduler)
        super().start()
        
        logger.info("DataLogger service started with queue consumer")

    def _queue_consumer_loop(self):
        """Consumer loop để đọc parsed values từ queue và update buffer"""
        logger.info("DataLogger queue consumer started")
        
        while not self._stop.is_set():
            try:
                # Lấy batch values từ datalogger queue
                raw_values = value_queue_service.get_datalogger_values_batch(max_count=100, timeout=0.5)
                
                if not raw_values:
                    continue
                
                # Update value buffer
                with self._buffer_lock:
                    for raw_value in raw_values:
                        # Parse value
                        try:
                            parsed_value = self._parse_raw_value(raw_value)
                            
                            if parsed_value is not None:
                                # Update buffer with latest value
                                self._value_buffer[raw_value.tag_id] = (raw_value.timestamp, parsed_value)
                                
                                # Also update cache for other services
                                self.cache.set(raw_value.tag_id, raw_value.timestamp, parsed_value)
                                
                        except Exception as e:
                            logger.error(f"Error parsing raw value for tag {raw_value.tag_name}: {e}")
                
                # Update stats
                value_queue_service.mark_logged(len(raw_values))
                
                with self.stats_lock:
                    self.stats['values_consumed'] += len(raw_values)
                
            except Exception as e:
                logger.error(f"Error in datalogger queue consumer: {e}")
                time.sleep(0.1)
        
        logger.info("DataLogger queue consumer stopped")

    def _parse_raw_value(self, raw_value: RawModbusValue) -> float:
        """
        Parse raw value using simplified logic
        Reuse parsing logic từ value_parser_service
        """
        from modbus_monitor.services.value_parser_service import ValueParserService
        
        # Create a temporary parser instance for parsing logic
        # Note: This is not ideal but reuses the parsing code
        parser = ValueParserService(self.cache)
        return parser._parse_single_value(raw_value)
