#!/usr/bin/env python3
"""
Standalone Alarm Worker Process - Simple launcher for alarm worker
All alarm logic and notifications are now handled within the alarm_worker.py
"""
import os
import sys
import time
import signal
import multiprocessing
from multiprocessing import Queue, Manager, Process

# Add project paths
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'webapp'))

# Tự thu nhỏ cửa sổ console ngay khi khởi động (bản --console). No-op nếu --noconsole.
try:
    from utils.console_window import minimize_console
    minimize_console()
except Exception:
    pass

# Centralized logging — tự tạo folder logs/, hoạt động cả khi build EXE
from shared.app_logger import get_logger, log_exception, log_startup
logger = get_logger("orchestra_alarm")

from workers.alarm_worker import AlarmConfig, create_alarm_worker_process

class AlarmWorkerLauncher:
    """Simple launcher for alarm worker process"""
    
    def __init__(self):
        self.alarm_process = None
        self.data_queue = Queue()
        self.command_queue = Queue()
        self.log_queue = Queue()
        self.shared_state = Manager().dict()
        self.running = False
        
        logger.info("Alarm Worker Launcher initialized")
        
    def start_alarm_worker(self):
        """Start the alarm worker process"""
        if self.alarm_process and self.alarm_process.is_alive():
            logger.warning("Alarm worker already running")
            return
        
        # Create alarm config - enable notifications in worker since all logic is there now
        config = AlarmConfig(
            check_interval=1.0,
            enable_notifications=True,  # Enable notifications in worker
            database_timeout=5.0
        )
        
        logger.info("Creating alarm worker process...")
        self.alarm_process = create_alarm_worker_process(
            config=config,
            data_queue=self.data_queue,
            command_queue=self.command_queue,
            log_queue=self.log_queue,
            shared_state=self.shared_state
        )
        
        logger.info("Starting alarm worker process...")
        self.alarm_process.start()
        self.running = True
        
        logger.info("Alarm worker started (PID: %s)", self.alarm_process.pid)
        
    def stop_alarm_worker(self):
        """Stop the alarm worker process"""
        if not self.alarm_process or not self.alarm_process.is_alive():
            logger.warning("Alarm worker not running")
            return
        
        logger.info("Stopping alarm worker...")
        
        # Send stop command
        try:
            self.command_queue.put({"type": "stop"}, timeout=1)
        except:
            pass
        
        # Wait for graceful shutdown
        self.alarm_process.join(timeout=5)
        
        # Force terminate if needed
        if self.alarm_process.is_alive():
            logger.warning("Force terminating alarm worker...")
            self.alarm_process.terminate()
            self.alarm_process.join(timeout=2)
        
        if self.alarm_process.is_alive():
            logger.warning("Killing alarm worker...")
            self.alarm_process.kill()
        
        self.running = False
        logger.info("Alarm worker stopped")
        
    def get_worker_status(self):
        """Get alarm worker status"""
        if not self.alarm_process or not self.alarm_process.is_alive():
            return {
                "running": False,
                "pid": None,
                "status": "Not running"
            }
        
        # Get status from shared state
        worker_status = self.shared_state.get("alarm_worker_status", {})
        
        return {
            "running": True,
            "pid": self.alarm_process.pid,
            "status": "Running",
            "seq": worker_status.get("seq", 0),
            "active_alarms": worker_status.get("active_alarms", 0),
            "total_rules": worker_status.get("total_rules", 0),
            "last_update": worker_status.get("last_update", 0)
        }
        
    def process_log_messages(self):
        """Process log messages from worker"""
        try:
            while not self.log_queue.empty():
                log_entry = self.log_queue.get_nowait()
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(log_entry.get('timestamp', time.time())))
                worker_id = log_entry.get('worker_id', 'alarm_worker')
                level = log_entry.get('level', 'INFO')
                message = log_entry.get('message', '')
                
                # Ghi vào file log tập trung thay vì chỉ print
                log_level = getattr(logger, level.lower(), logger.info)
                log_level("[%s] %s", worker_id, message)
        except:
            pass  # Queue empty
            
    def run_main_loop(self):
        """Main monitoring loop"""
        logger.info("Starting main monitoring loop...")
        
        try:
            while self.running:
                # Process log messages
                self.process_log_messages()
                
                # Check if worker is still alive
                if self.alarm_process and not self.alarm_process.is_alive():
                    exit_code = self.alarm_process.exitcode
                    logger.error("Alarm worker process died unexpectedly (exit code: %s)", exit_code)
                    break
                
                time.sleep(1)  # Check every second
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            log_exception("orchestra_alarm", e, "Error in main loop")
        finally:
            self.stop_alarm_worker()

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Received signal %s", signum)
    # The main loop will handle cleanup
    
def main():
    """Main entry point"""
    # --- Single Instance Check ---
    from utils.single_instance import ensure_single_instance
    _instance_lock = ensure_single_instance("orchestra_alarm")

    # Log system info at startup (EXE-safe)
    log_startup("orchestra_alarm", "Modbus Monitor - Alarm Worker Service")
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start launcher
    launcher = AlarmWorkerLauncher()
    
    try:
        launcher.start_alarm_worker()
        launcher.run_main_loop()
    except Exception as e:
        log_exception("orchestra_alarm", e, "Fatal error")
    finally:
        launcher.stop_alarm_worker()
        logger.info("Alarm worker service shutdown complete")

if __name__ == "__main__":
    # Ensure proper multiprocessing setup on Windows
    if sys.platform.startswith('win'):
        multiprocessing.freeze_support()
    
    main()