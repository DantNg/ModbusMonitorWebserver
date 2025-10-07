"""
DataLogger Worker - Dedicated process for data logging operations
"""
import time
import logging
import threading
from multiprocessing import Process, Queue
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass 
class DataLoggerConfig:
    """Configuration for datalogger worker"""
    check_interval: float = 1.0  # How often to check for logging tasks
    buffer_size: int = 1000      # Maximum buffered values
    batch_size: int = 100        # Values to write per batch

class DataLoggerWorker:
    """Dedicated worker process for data logging"""
    
    def __init__(self, config: DataLoggerConfig, data_queue: Queue, command_queue: Queue, 
                 log_queue: Queue, shared_state):
        self.config = config
        self.data_queue = data_queue
        self.command_queue = command_queue
        self.log_queue = log_queue
        self.shared_state = shared_state
        
        # Logger configurations (logger_id -> config)
        self._logger_configs: Dict[int, dict] = {}
        
        # Scheduling (logger_id -> next_run_time)
        self._next_runs: Dict[int, float] = {}
        self._intervals: Dict[int, float] = {}
        
        # Value buffer (tag_id -> (timestamp, value))
        self._value_buffer: Dict[int, Tuple[float, float]] = {}
        self._buffer_lock = threading.Lock()
        
        # Runtime state
        self.running = False
        self.seq = 0
        
        # Statistics
        self.stats = {
            'values_consumed': 0,
            'values_logged': 0,
            'log_executions': 0,
            'start_time': time.time()
        }
    
    def log(self, level, message):
        """Send log message to main process"""
        try:
            self.log_queue.put({
                "worker_id": "datalogger_worker", 
                "level": level,
                "message": message,
                "timestamp": time.time()
            }, block=False)
        except:
            pass  # Queue full
    
    def _load_logger_configs(self) -> List[dict]:
        """Load data logger configurations from database"""
        try:
            # TODO: Load from shared database or cache
            # For now, return empty list
            return []
        except Exception as e:
            self.log("ERROR", f"Failed to load logger configs: {e}")
            return []
    
    def _reload_configs(self):
        """Reload logger configurations and reset scheduling"""
        try:
            loggers = self._load_logger_configs()
            now = time.monotonic()
            
            self._logger_configs.clear()
            self._intervals.clear()
            self._next_runs.clear()
            
            for logger_config in loggers:
                logger_id = logger_config['id']
                interval_sec = float(logger_config.get('interval_sec', 60.0))
                
                self._logger_configs[logger_id] = logger_config
                self._intervals[logger_id] = interval_sec
                self._next_runs[logger_id] = now + 0.1  # Start soon
            
            self.log("INFO", f"Reloaded {len(loggers)} logger configurations")
            
        except Exception as e:
            self.log("ERROR", f"Failed to reload logger configs: {e}")
    
    def _consume_data_queue(self):
        """Consume values from data queue and buffer them"""
        try:
            while not self.data_queue.empty():
                data = self.data_queue.get_nowait()
                
                if data["type"] == "tag_value":
                    tag_id = data["tag_id"]
                    timestamp = data["timestamp"]
                    value = data["value"]
                    
                    # Buffer the value
                    with self._buffer_lock:
                        self._value_buffer[tag_id] = (timestamp, value)
                    
                    self.stats['values_consumed'] += 1
                    
        except Exception as e:
            if "Empty" not in str(e):  # Ignore empty queue errors
                self.log("ERROR", f"Error consuming data queue: {e}")
    
    def _get_tag_values_for_logger(self, logger_config: dict) -> Dict[int, Tuple[float, float]]:
        """Get current values for all tags in a logger configuration"""
        tag_values = {}
        tag_ids = logger_config.get("tag_ids", [])
        
        with self._buffer_lock:
            for tag_id in tag_ids:
                if tag_id in self._value_buffer:
                    tag_values[tag_id] = self._value_buffer[tag_id]
        
        return tag_values
    
    def _write_log_entry(self, logger_id: int, tag_values: Dict[int, Tuple[float, float]]):
        """Write a single log entry to database"""
        try:
            if not tag_values:
                return
            
            # TODO: Implement database writing
            # For now, just log the action
            self.log("INFO", f"Logger {logger_id}: Writing {len(tag_values)} tag values")
            
            # Update statistics
            self.stats['values_logged'] += len(tag_values)
            self.stats['log_executions'] += 1
            
        except Exception as e:
            self.log("ERROR", f"Failed to write log entry for logger {logger_id}: {e}")
    
    def _execute_logger(self, logger_id: int, logger_config: dict):
        """Execute a single data logger"""
        try:
            # Get current tag values
            tag_values = self._get_tag_values_for_logger(logger_config)
            
            if tag_values:
                # Write to database
                self._write_log_entry(logger_id, tag_values)
                
                # Update next run time (precise scheduling to prevent drift)
                interval = self._intervals[logger_id]
                self._next_runs[logger_id] += interval
                
                # If we're behind schedule, catch up
                now = time.monotonic()
                if self._next_runs[logger_id] < now:
                    self._next_runs[logger_id] = now + interval
                    
            else:
                # No data available, reschedule for soon
                self._next_runs[logger_id] = time.monotonic() + 1.0
                
        except Exception as e:
            self.log("ERROR", f"Failed to execute logger {logger_id}: {e}")
            # Reschedule for later
            self._next_runs[logger_id] = time.monotonic() + self._intervals.get(logger_id, 60.0)
    
    def _datalogger_loop(self):
        """Main data logging loop"""
        self.log("INFO", "Starting data logger loop")
        
        # Initial config load
        self._reload_configs()
        last_config_reload = time.time()
        
        while self.running:
            try:
                # Consume new data from queue
                self._consume_data_queue()
                
                # Reload configs periodically (every 30 seconds)
                if time.time() - last_config_reload > 30:
                    self._reload_configs()
                    last_config_reload = time.time()
                
                # Check which loggers need to run
                now = time.monotonic()
                
                for logger_id, next_run in list(self._next_runs.items()):
                    if now >= next_run:
                        logger_config = self._logger_configs.get(logger_id)
                        if logger_config and logger_config.get("enabled", True):
                            self._execute_logger(logger_id, logger_config)
                
                # Update sequence number
                self.seq += 1
                
                # Sleep briefly
                time.sleep(self.config.check_interval)
                
            except Exception as e:
                self.log("ERROR", f"Error in datalogger loop: {e}")
                time.sleep(1)  # Prevent rapid error loops
    
    def _handle_commands(self):
        """Handle commands from main process"""
        try:
            while not self.command_queue.empty():
                command = self.command_queue.get_nowait()
                
                if command["type"] == "stop":
                    self.log("INFO", "Received stop command")
                    self.running = False
                    
                elif command["type"] == "reload_config":
                    self.log("INFO", "Received reload config command")
                    self._reload_configs()
                    
                elif command["type"] == "get_status":
                    # Send status back via shared state
                    self.shared_state["datalogger_worker_status"] = {
                        "running": self.running,
                        "seq": self.seq,
                        "active_loggers": len(self._logger_configs),
                        "buffered_values": len(self._value_buffer),
                        "stats": self.stats.copy(),
                        "last_update": time.time()
                    }
                    
                elif command["type"] == "force_log":
                    # Force immediate logging for a specific logger
                    logger_id = command.get("logger_id")
                    if logger_id in self._logger_configs:
                        self._execute_logger(logger_id, self._logger_configs[logger_id])
                        
        except Exception as e:
            if "Empty" not in str(e):  # Ignore empty queue errors
                self.log("ERROR", f"Error handling commands: {e}")
    
    def run(self):
        """Main worker process entry point"""
        self.log("INFO", "DataLogger worker starting")
        self.running = True
        
        try:
            # Start data logging loop
            self._datalogger_loop()
            
        except Exception as e:
            self.log("ERROR", f"Fatal error in datalogger worker: {e}")
        finally:
            self.running = False
            self.log("INFO", "DataLogger worker stopped")

def datalogger_worker_main(config, data_queue, command_queue, log_queue, shared_state):
    """Main entry point for datalogger worker process"""
    worker = DataLoggerWorker(config, data_queue, command_queue, log_queue, shared_state)
    
    # Handle commands periodically
    def command_handler():
        while worker.running:
            worker._handle_commands()
            time.sleep(0.1)
    
    # Start command handler thread
    command_thread = threading.Thread(target=command_handler, daemon=True)
    command_thread.start()
    
    # Run main worker
    worker.run()

def create_datalogger_worker_process(config: DataLoggerConfig, data_queue: Queue, command_queue: Queue,
                                    log_queue: Queue, shared_state) -> Process:
    """Create and return datalogger worker process"""
    return Process(
        target=datalogger_worker_main,
        args=(config, data_queue, command_queue, log_queue, shared_state), 
        name="DataLoggerWorker"
    )