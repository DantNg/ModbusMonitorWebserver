"""
Logger Worker - Individual datalogger process
Reads from tag_latest_values and writes to tag_logs according to logger configuration
"""
import sys
import os
import time
import threading
import logging
from multiprocessing import Process, Queue
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional
import traceback

# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'webapp'))

# Import database functions
try:
    from webapp.modbus_monitor.database.db import (
        init_engine, get_latest_tag_values_batch, bulk_insert_tag_logs, 
        purge_tag_logs_by_logger, get_data_logger, list_data_logger_tags,
        safe_datetime_now
    )
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you're running from the flask_modbus_monitor directory")
    print("Current directory:", os.getcwd())
    sys.exit(1)

class LoggerWorker:
    """Individual datalogger worker process"""
    
    def __init__(self, logger_id: int, command_queue: Queue, log_queue: Queue):
        self.logger_id = logger_id
        self.command_queue = command_queue
        self.log_queue = log_queue
        
        # Configuration
        self.interval_sec = 60.0  # Default interval
        self.tag_ids: List[int] = []
        self.enabled = True
        
        # Runtime state
        self.running = False
        self.last_run = 0.0
        self.sequence = 0
        
        # Statistics
        self.stats = {
            'fetch_count': 0,
            'insert_count': 0,
            'error_count': 0,
            'last_fetch_ms': 0,
            'last_insert_ms': 0,
            'last_lag_ms': 0,
            'total_rows_inserted': 0,
            'start_time': time.time()
        }
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"logger_worker_{logger_id}")
    
    def log_message(self, level: str, message: str):
        """Send log message to parent process"""
        try:
            self.log_queue.put({
                'worker_id': f'logger_{self.logger_id}',
                'level': level,
                'message': message,
                'timestamp': time.time(),
                'stats': self.stats.copy()
            }, block=False)
        except:
            pass  # Queue might be full
    
    def load_configuration(self):
        """Load logger configuration from database"""
        try:
            # Get logger config
            logger_config = get_data_logger(self.logger_id)
            if not logger_config:
                self.log_message("ERROR", f"Logger {self.logger_id} not found in database")
                return False
            
            # Update configuration
            self.interval_sec = float(logger_config.get('interval_sec', 60.0))
            self.enabled = bool(logger_config.get('enabled', True))
            
            # Get associated tag IDs
            self.tag_ids = list_data_logger_tags(self.logger_id)
            
            self.log_message("INFO", f"Loaded config: interval={self.interval_sec}s, tags={len(self.tag_ids)}, enabled={self.enabled}")
            return True
            
        except Exception as e:
            self.log_message("ERROR", f"Failed to load configuration: {e}")
            return False
    
    def fetch_tag_values(self) -> List[Tuple[int, float, datetime]]:
        """Fetch current values for all configured tags"""
        if not self.tag_ids:
            return []
        
        start_time = time.time()
        
        try:
            # Get latest values for all tags in batch
            tag_values = get_latest_tag_values_batch(self.tag_ids)
            
            # Convert to list of tuples (tag_id, value, timestamp)
            result = []
            for tag_id in self.tag_ids:
                if tag_id in tag_values:
                    value, timestamp = tag_values[tag_id]
                    
                    # Skip tags with None values or timestamps
                    if value is not None and timestamp is not None:
                        # Ensure value is numeric
                        try:
                            numeric_value = float(value)
                            result.append((tag_id, numeric_value, timestamp))
                        except (ValueError, TypeError):
                            self.log_message("WARNING", f"Tag {tag_id}: Invalid value {value}, skipping")
                            continue
                    else:
                        self.log_message("DEBUG", f"Tag {tag_id}: No data available (value={value}, ts={timestamp})")
            
            fetch_time_ms = (time.time() - start_time) * 1000
            self.stats['last_fetch_ms'] = fetch_time_ms
            self.stats['fetch_count'] += 1
            
            # Calculate lag (time difference between now and most recent data)
            if result:
                now = safe_datetime_now()
                max_timestamp = max(ts for _, _, ts in result)
                lag_ms = (now - max_timestamp).total_seconds() * 1000
                self.stats['last_lag_ms'] = lag_ms
            
            self.log_message("DEBUG", f"Fetched {len(result)} valid tag values in {fetch_time_ms:.1f}ms")
            return result
            
        except Exception as e:
            self.stats['error_count'] += 1
            self.log_message("ERROR", f"Failed to fetch tag values: {e}")
            import traceback
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}")
            return []
    
    def insert_log_entries(self, tag_values: List[Tuple[int, float, datetime]]) -> int:
        """Insert tag values into tag_logs table"""
        if not tag_values:
            return 0
        
        start_time = time.time()
        
        try:
            # Sử dụng 1 timestamp THỐNG NHẤT cho tất cả tags trong lần capture này
            # Điều này đảm bảo:
            # 1. Interval chính xác theo cấu hình
            # 2. Tất cả tags được ghi cùng 1 timestamp → không bị ô trống
            unified_timestamp = safe_datetime_now()
            
            # Convert to format expected by bulk_insert_tag_logs
            # (logger_id, tag_id, ts, value)
            log_entries = []
            for tag_id, value, _ in tag_values:  # Bỏ qua timestamp cũ từ tag_latest_values
                log_entries.append((self.logger_id, tag_id, unified_timestamp, float(value)))
            
            # Debug log
            self.log_message("DEBUG", f"Prepared {len(log_entries)} log entries with unified timestamp {unified_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Bulk insert with idempotency
            rows_affected = bulk_insert_tag_logs(log_entries)
            
            insert_time_ms = (time.time() - start_time) * 1000
            self.stats['last_insert_ms'] = insert_time_ms
            self.stats['insert_count'] += 1
            self.stats['total_rows_inserted'] += rows_affected
            
            self.log_message("DEBUG", f"Inserted {rows_affected} rows in {insert_time_ms:.1f}ms")
            return rows_affected
            
        except Exception as e:
            self.stats['error_count'] += 1
            self.log_message("ERROR", f"Failed to insert log entries: {e}")
            import traceback
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}")
            return 0
    
    def handle_commands(self):
        """Process commands from parent"""
        try:
            while not self.command_queue.empty():
                command = self.command_queue.get_nowait()
                
                if command['type'] == 'stop':
                    self.log_message("INFO", "Received stop command")
                    self.running = False
                    
                elif command['type'] == 'reload_config':
                    self.log_message("INFO", "Received reload_config command")
                    self.load_configuration()
                    
                elif command['type'] == 'purge_range':
                    dt_from = command.get('dt_from')
                    dt_to = command.get('dt_to')
                    self.log_message("INFO", f"Received purge_range command: {dt_from} to {dt_to}")
                    
                    try:
                        deleted_count = purge_tag_logs_by_logger(self.logger_id, dt_from, dt_to)
                        self.log_message("INFO", f"Purged {deleted_count} log entries")
                    except Exception as e:
                        self.log_message("ERROR", f"Failed to purge logs: {e}")
                        
                elif command['type'] == 'get_stats':
                    # Send current stats
                    self.log_message("INFO", f"Stats: {self.stats}")
                    
        except Exception as e:
            if "Empty" not in str(e):
                self.log_message("ERROR", f"Error handling commands: {e}")
    
    def should_run_now(self) -> bool:
        """Check if it's time to run the logging cycle"""
        if not self.enabled:
            return False
            
        now = time.time()
        return (now - self.last_run) >= self.interval_sec
    
    def run_logging_cycle(self):
        """Execute one logging cycle"""
        try:
            # Fetch current tag values
            tag_values = self.fetch_tag_values()
            
            if tag_values:
                # Insert into database
                rows_inserted = self.insert_log_entries(tag_values)
                
                self.log_message("INFO", 
                    f"Cycle {self.sequence}: {len(tag_values)} tags → {rows_inserted} rows "
                    f"(fetch: {self.stats['last_fetch_ms']:.1f}ms, "
                    f"insert: {self.stats['last_insert_ms']:.1f}ms, "
                    f"lag: {self.stats['last_lag_ms']:.1f}ms)")
            else:
                self.log_message("WARNING", f"Cycle {self.sequence}: No tag values fetched")
            
            # Update timing
            self.last_run = time.time()
            self.sequence += 1
            
        except Exception as e:
            self.stats['error_count'] += 1
            self.log_message("ERROR", f"Error in logging cycle: {e}")
            traceback.print_exc()
    
    def run(self):
        """Main worker loop"""
        self.log_message("INFO", f"Starting logger worker for logger_id={self.logger_id}")
        
        # Load initial configuration
        if not self.load_configuration():
            self.log_message("ERROR", "Failed to load initial configuration, exiting")
            return
        
        self.running = True
        
        try:
            while self.running:
                # Handle commands from parent
                self.handle_commands()
                
                # Check if it's time to run logging cycle
                if self.should_run_now():
                    self.run_logging_cycle()
                
                # Sleep briefly to avoid busy waiting
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            self.log_message("INFO", "Interrupted by user")
        except Exception as e:
            self.log_message("ERROR", f"Fatal error in main loop: {e}")
            traceback.print_exc()
        finally:
            self.running = False
            self.log_message("INFO", f"Logger worker stopped. Total cycles: {self.sequence}")

def logger_worker_main(logger_id: int, command_queue: Queue, log_queue: Queue):
    """Entry point for logger worker process"""
    worker = LoggerWorker(logger_id, command_queue, log_queue)
    worker.run()

def create_logger_worker_process(logger_id: int, command_queue: Queue, log_queue: Queue) -> Process:
    """Create and return a logger worker process"""
    return Process(
        target=logger_worker_main,
        args=(logger_id, command_queue, log_queue),
        name=f"LoggerWorker-{logger_id}"
    )

if __name__ == "__main__":
    # For testing - run single logger worker
    if len(sys.argv) != 2:
        print("Usage: python logger_worker.py <logger_id>")
        sys.exit(1)
    
    logger_id = int(sys.argv[1])
    
    # Create dummy queues for testing
    from multiprocessing import Queue
    command_queue = Queue()
    log_queue = Queue()
    
    print(f"Starting logger worker for logger_id={logger_id}")
    worker = LoggerWorker(logger_id, command_queue, log_queue)
    worker.run()