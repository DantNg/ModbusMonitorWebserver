"""
Orchestra Datalogger - Parent process managing multiple datalogger workers
Manages lifecycle of logger worker processes based on data_loggers configuration
"""
import sys
import os
import time
import threading
import signal
import logging
from multiprocessing import Process, Queue, Manager
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import traceback

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'webapp'))

# Import components
try:
    from workers.logger_worker import create_logger_worker_process
    from webapp.modbus_monitor.database.db import (
        init_engine, list_data_loggers, safe_datetime_now
    )
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you're running from the flask_modbus_monitor directory")
    print("Current directory:", os.getcwd())
    print("Script directory:", current_dir)
    sys.exit(1)

class OrchestraDatalogger:
    """Parent process managing multiple datalogger workers"""
    
    def __init__(self, check_interval: float = 10.0):
        self.check_interval = check_interval  # How often to check for config changes
        
        # Worker management
        self.workers: Dict[int, Dict] = {}  # logger_id -> worker info
        self.command_queues: Dict[int, Queue] = {}  # logger_id -> command queue
        self.log_queue = Queue()  # Shared log queue for all workers
        
        # Runtime state
        self.running = False
        self.manager = Manager()
        self.shared_state = self.manager.dict()
        
        # Statistics
        self.stats = {
            'active_workers': 0,
            'total_cycles': 0,
            'config_reloads': 0,
            'worker_restarts': 0,
            'start_time': time.time()
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("orchestra_datalogger")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def get_enabled_loggers(self) -> List[Dict]:
        """Get list of enabled dataloggers from database"""
        try:
            loggers = list_data_loggers()
            return [logger for logger in loggers if logger.get('enabled', True)]
        except Exception as e:
            self.logger.error(f"Failed to get enabled loggers: {e}")
            return []
    
    def start_worker(self, logger_id: int) -> bool:
        """Start a new worker process for given logger_id"""
        try:
            if logger_id in self.workers:
                self.logger.warning(f"Worker for logger {logger_id} already exists")
                return False
            
            # Create command queue for this worker
            command_queue = Queue()
            self.command_queues[logger_id] = command_queue
            
            # Create and start worker process
            process = create_logger_worker_process(logger_id, command_queue, self.log_queue)
            process.start()
            
            # Store worker info
            self.workers[logger_id] = {
                'process': process,
                'started_at': time.time(),
                'last_seen': time.time(),
                'restart_count': 0
            }
            
            self.stats['active_workers'] = len(self.workers)
            self.logger.info(f"Started worker for logger {logger_id} (PID: {process.pid})")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start worker for logger {logger_id}: {e}")
            return False
    
    def stop_worker(self, logger_id: int, timeout: float = 5.0) -> bool:
        """Stop worker process for given logger_id"""
        if logger_id not in self.workers:
            return True
        
        try:
            worker_info = self.workers[logger_id]
            process = worker_info['process']
            
            # Send stop command
            if logger_id in self.command_queues:
                try:
                    self.command_queues[logger_id].put({'type': 'stop'}, timeout=1.0)
                except:
                    pass  # Queue might be full
            
            # Wait for graceful shutdown
            process.join(timeout=timeout)
            
            # Force terminate if still alive
            if process.is_alive():
                self.logger.warning(f"Force terminating worker {logger_id}")
                process.terminate()
                process.join(timeout=2.0)
                
                if process.is_alive():
                    self.logger.error(f"Failed to terminate worker {logger_id}")
                    process.kill()
            
            # Cleanup
            del self.workers[logger_id]
            if logger_id in self.command_queues:
                del self.command_queues[logger_id]
            
            self.stats['active_workers'] = len(self.workers)
            self.logger.info(f"Stopped worker for logger {logger_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop worker for logger {logger_id}: {e}")
            return False
    
    def restart_worker(self, logger_id: int) -> bool:
        """Restart worker for given logger_id"""
        self.logger.info(f"Restarting worker for logger {logger_id}")
        
        # Stop existing worker
        self.stop_worker(logger_id)
        
        # Start new worker
        if self.start_worker(logger_id):
            if logger_id in self.workers:
                self.workers[logger_id]['restart_count'] += 1
            self.stats['worker_restarts'] += 1
            return True
        
        return False
    
    def send_command_to_worker(self, logger_id: int, command: Dict) -> bool:
        """Send command to specific worker"""
        if logger_id not in self.command_queues:
            self.logger.error(f"No command queue for worker {logger_id}")
            return False
        
        try:
            self.command_queues[logger_id].put(command, timeout=1.0)
            self.logger.debug(f"Sent command {command['type']} to worker {logger_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send command to worker {logger_id}: {e}")
            return False
    
    def reload_worker_configs(self):
        """Send reload_config command to all workers"""
        self.logger.info("Reloading configurations for all workers")
        
        for logger_id in list(self.workers.keys()):
            self.send_command_to_worker(logger_id, {'type': 'reload_config'})
        
        self.stats['config_reloads'] += 1
    
    def purge_worker_logs(self, logger_id: int, dt_from: datetime = None, dt_to: datetime = None) -> bool:
        """Send purge command to specific worker"""
        command = {
            'type': 'purge_range',
            'dt_from': dt_from,
            'dt_to': dt_to
        }
        return self.send_command_to_worker(logger_id, command)
    
    def check_worker_health(self):
        """Check if all workers are running and restart if needed"""
        dead_workers = []
        
        for logger_id, worker_info in self.workers.items():
            process = worker_info['process']
            
            if not process.is_alive():
                self.logger.warning(f"Worker {logger_id} is dead (exit code: {process.exitcode})")
                dead_workers.append(logger_id)
        
        # Restart dead workers
        for logger_id in dead_workers:
            self.restart_worker(logger_id)
    
    def sync_workers_with_config(self):
        """Synchronize running workers with database configuration"""
        try:
            enabled_loggers = self.get_enabled_loggers()
            enabled_ids = {logger['id'] for logger in enabled_loggers}
            running_ids = set(self.workers.keys())
            
            # Start workers for new loggers
            to_start = enabled_ids - running_ids
            for logger_id in to_start:
                self.start_worker(logger_id)
            
            # Stop workers for disabled/deleted loggers
            to_stop = running_ids - enabled_ids
            for logger_id in to_stop:
                self.stop_worker(logger_id)
            
            if to_start or to_stop:
                self.logger.info(f"Config sync: started {len(to_start)}, stopped {len(to_stop)} workers")
                
        except Exception as e:
            self.logger.error(f"Failed to sync workers with config: {e}")
    
    def process_log_messages(self):
        """Process log messages from worker processes"""
        try:
            while not self.log_queue.empty():
                log_data = self.log_queue.get_nowait()
                
                worker_id = log_data.get('worker_id', 'unknown')
                level = log_data.get('level', 'INFO')
                message = log_data.get('message', '')
                timestamp = log_data.get('timestamp', time.time())
                stats = log_data.get('stats', {})
                
                # Log the message
                log_level = getattr(logging, level.upper(), logging.INFO)
                self.logger.log(log_level, f"[{worker_id}] {message}")
                
                # Update worker stats if available
                if 'logger_' in worker_id:
                    try:
                        logger_id = int(worker_id.split('_')[1])
                        if logger_id in self.workers:
                            self.workers[logger_id]['last_seen'] = timestamp
                            self.workers[logger_id]['stats'] = stats
                    except:
                        pass
                        
        except Exception as e:
            if "Empty" not in str(e):
                self.logger.error(f"Error processing log messages: {e}")
    
    def print_status(self):
        """Print current status of all workers"""
        print(f"\n{'='*60}")
        print(f"Orchestra Datalogger Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        print(f"Running: {self.running}")
        print(f"Active workers: {self.stats['active_workers']}")
        print(f"Total cycles: {self.stats['total_cycles']}")
        print(f"Config reloads: {self.stats['config_reloads']}")
        print(f"Worker restarts: {self.stats['worker_restarts']}")
        print(f"Uptime: {time.time() - self.stats['start_time']:.1f}s")
        
        if self.workers:
            print(f"\nWorker Details:")
            print(f"{'Logger ID':<10} {'PID':<8} {'Status':<10} {'Uptime':<10} {'Restarts':<10}")
            print(f"{'-'*60}")
            
            for logger_id, worker_info in self.workers.items():
                process = worker_info['process']
                uptime = time.time() - worker_info['started_at']
                status = "Running" if process.is_alive() else "Dead"
                
                print(f"{logger_id:<10} {process.pid:<8} {status:<10} {uptime:<10.1f} {worker_info['restart_count']:<10}")
        
        print(f"{'='*60}\n")
    
    def run(self):
        """Main orchestra loop"""
        self.logger.info("Starting Orchestra Datalogger")
        self.running = True
        
        # Initial worker sync
        self.sync_workers_with_config()
        
        last_config_check = 0
        last_status_print = 0
        
        try:
            while self.running:
                current_time = time.time()
                
                # Process log messages from workers
                self.process_log_messages()
                
                # Check worker health
                self.check_worker_health()
                
                # Periodic config sync
                if current_time - last_config_check >= self.check_interval:
                    self.sync_workers_with_config()
                    last_config_check = current_time
                
                # Print status every 60 seconds
                if current_time - last_status_print >= 60:
                    self.print_status()
                    last_status_print = current_time
                
                self.stats['total_cycles'] += 1
                
                # Sleep briefly
                time.sleep(1.0)
                
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user")
        except Exception as e:
            self.logger.error(f"Fatal error in main loop: {e}")
            traceback.print_exc()
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown"""
        self.logger.info("Shutting down Orchestra Datalogger...")
        self.running = False
        
        # Stop all workers
        for logger_id in list(self.workers.keys()):
            self.stop_worker(logger_id, timeout=3.0)
        
        self.logger.info("Orchestra Datalogger stopped")
    
    # API methods for external control
    def api_reload_config(self):
        """API: Reload configuration for all workers"""
        self.reload_worker_configs()
        return {"status": "success", "message": "Config reload requested for all workers"}
    
    def api_purge_logs(self, logger_id: int, days_back: int = None, dt_from: datetime = None, dt_to: datetime = None):
        """API: Purge logs for specific logger"""
        if days_back and not dt_from:
            dt_from = safe_datetime_now() - timedelta(days=days_back)
        
        if not dt_to:
            dt_to = safe_datetime_now()
        
        success = self.purge_worker_logs(logger_id, dt_from, dt_to)
        return {
            "status": "success" if success else "error",
            "message": f"Purge requested for logger {logger_id}"
        }
    
    def api_get_status(self):
        """API: Get current status"""
        workers_status = {}
        for logger_id, worker_info in self.workers.items():
            process = worker_info['process']
            workers_status[logger_id] = {
                'pid': process.pid,
                'alive': process.is_alive(),
                'started_at': worker_info['started_at'],
                'last_seen': worker_info['last_seen'],
                'restart_count': worker_info['restart_count'],
                'stats': worker_info.get('stats', {})
            }
        
        return {
            'orchestra_stats': self.stats,
            'workers': workers_status,
            'timestamp': time.time()
        }

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Orchestra Datalogger - Manage multiple datalogger workers')
    parser.add_argument('--check-interval', type=float, default=10.0, 
                       help='Config check interval in seconds (default: 10.0)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create and run orchestra
    orchestra = OrchestraDatalogger(check_interval=args.check_interval)
    
    try:
        orchestra.run()
    except Exception as e:
        print(f"Failed to start orchestra: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()