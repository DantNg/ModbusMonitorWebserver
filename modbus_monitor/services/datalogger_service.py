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
        self._stop_event = threading.Event()
        
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
        
        while not self._stop_event.is_set():
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

    def run(self):
        """Main scheduler loop - ghi database theo intervals của từng logger"""
        logger.info("DataLogger scheduler started")
        
        try:
            while not self._stop_event.is_set():
                now = time.monotonic()
                
                # Load loggers và check intervals
                loggers = self._load_loggers()
                
                for logger_config in loggers:
                    logger_id = logger_config['id']
                    interval_sec = float(logger_config.get('interval_sec', 60.0))
                    
                    # Initialize next run time nếu chưa có
                    if logger_id not in self._next_runs:
                        self._next_runs[logger_id] = now + 0.1  # Start soon
                        self._intervals[logger_id] = interval_sec
                    
                    # Check if interval changed
                    if self._intervals.get(logger_id) != interval_sec:
                        logger.debug(f"Logger {logger_id} interval changed to {interval_sec}s")
                        self._intervals[logger_id] = interval_sec
                        self._next_runs[logger_id] = now + 0.1  # Restart soon
                        continue
                    
                    # Check nếu đến thời gian log
                    if now >= self._next_runs[logger_id]:
                        self._execute_logger(logger_config)
                        
                        # Schedule next run (anti-drift)
                        self._next_runs[logger_id] += interval_sec
                        
                        # Skip missed cycles to catch up (but not too many)
                        max_catchup = 3
                        catchup_count = 0
                        while self._next_runs[logger_id] <= now and catchup_count < max_catchup:
                            self._next_runs[logger_id] += interval_sec
                            catchup_count += 1
                
                # Sleep ngắn để không waste CPU
                self._stop_event.wait(0.1)
                
        except Exception as e:
            logger.error(f"DataLogger scheduler error: {e}")
        finally:
            logger.info("DataLogger scheduler stopped")

    def _load_loggers(self) -> list[dict]:
        """Load logger configurations từ database"""
        try:
            # Get all active loggers
            loggers = dbsync.list_data_loggers()
            return [l for l in loggers if l.get('enabled', True)]
        except Exception as e:
            logger.error(f"Error loading loggers: {e}")
            return []

    def _execute_logger(self, logger_config: dict):
        """Execute một logger - ghi data của các tags thuộc logger này"""
        logger_id = logger_config['id']
        logger_name = logger_config.get('name', f'Logger-{logger_id}')
        
        try:
            # Get tags thuộc logger này
            tag_ids = dbsync.get_logger_tag_ids(logger_id)
            
            if not tag_ids:
                logger.debug(f"No tags assigned to logger {logger_name}")
                return
            
            # Get latest values từ buffer
            values_to_log = []
            current_time = time.time()
            
            with self._buffer_lock:
                for tag_id in tag_ids:
                    if tag_id in self._value_buffer:
                        timestamp, value = self._value_buffer[tag_id]
                        values_to_log.append({
                            'tag_id': tag_id,
                            'timestamp': timestamp,
                            'value': value
                        })
                    else:
                        # No data available for this tag yet
                        logger.debug(f"No data available for tag {tag_id} in logger {logger_name}")
            
            if not values_to_log:
                logger.debug(f"No values to log for logger {logger_name}")
                return
            
            # Batch insert vào database
            self._batch_insert_logs(logger_id, values_to_log)
            
            # Update stats
            with self.stats_lock:
                self.stats['values_logged'] += len(values_to_log)
                self.stats['log_executions'] += 1
            
            logger.info(f"✅ {logger_name}: Logged {len(values_to_log)} values")
            
        except Exception as e:
            logger.error(f"❌ {logger_name}: Error - {e}")

    def _batch_insert_logs(self, logger_id: int, values: list[dict]):
        """Batch insert log entries vào database"""
        try:
            # Prepare data for batch insert
            log_entries = []
            
            for value_data in values:
                log_entries.append({
                    'tag_id': value_data['tag_id'],
                    'timestamp': value_data['timestamp'],
                    'value': value_data['value']
                })
            
            # Batch insert using database function
            dbsync.batch_insert_data_logs(log_entries)
            
            logger.debug(f"Batch inserted {len(log_entries)} log entries for logger {logger_id}")
            
        except Exception as e:
            logger.error(f"Error batch inserting logs for logger {logger_id}: {e}")

    def stop(self):
        """Stop service gracefully"""
        logger.info("🛑 Stopping DataLogger service...")
        self._stop_event.set()
        
        # Wait for consumer thread
        if self._queue_consumer_thread and self._queue_consumer_thread.is_alive():
            self._queue_consumer_thread.join(timeout=5.0)
        
        # Wait for main thread
        if self.is_alive():
            self.join(timeout=5.0)
        
        logger.info("✅ DataLogger service stopped")

    def get_stats(self) -> dict:
        """Get service statistics"""
        with self.stats_lock:
            runtime = time.time() - self.stats['start_time']
            stats = self.stats.copy()
            stats['runtime_seconds'] = runtime
            stats['values_per_second'] = stats['values_consumed'] / max(runtime, 1)
            stats['logs_per_second'] = stats['values_logged'] / max(runtime, 1)
            
            # Add buffer stats
            with self._buffer_lock:
                stats['buffer_size'] = len(self._value_buffer)
            
            # Add queue stats
            try:
                stats['queue_stats'] = value_queue_service.get_stats()
            except Exception:
                stats['queue_stats'] = {}
            
            return stats
