"""
Tag Status Monitor Service - Auto-disable tags after timeout
"""
import threading
import time
import logging
from typing import Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class TagStatusMonitor:
    """Monitor tag timestamps and auto-disable inactive tags"""
    
    def __init__(self, timeout_seconds: int = 30):
        self.timeout_seconds = timeout_seconds
        self.tag_last_seen: Dict[int, float] = {}  # tag_id -> last timestamp
        self.inactive_tags: set = set()  # Set of tag IDs that are inactive
        
        self._running = False
        self._thread: threading.Thread = None
        self._lock = threading.RLock()
        
        logger.info(f"Tag Status Monitor initialized with {timeout_seconds}s timeout")
    
    def update_tag_timestamp(self, tag_id: int, timestamp: float):
        """Update the last seen timestamp for a tag"""
        with self._lock:
            self.tag_last_seen[tag_id] = timestamp
            
            # If tag was inactive, mark it as active again
            if tag_id in self.inactive_tags:
                self.inactive_tags.remove(tag_id)
                logger.info(f"Tag {tag_id} is now active again")
    
    def is_tag_active(self, tag_id: int) -> bool:
        """Check if a tag is considered active (received data within timeout)"""
        with self._lock:
            return tag_id not in self.inactive_tags
    
    def get_inactive_tags(self) -> set:
        """Get set of inactive tag IDs"""
        with self._lock:
            return self.inactive_tags.copy()
    
    def start(self):
        """Start the monitoring thread"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        logger.info("Tag Status Monitor started")
    
    def stop(self):
        """Stop the monitoring thread"""
        if not self._running:
            return
        
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        
        logger.info("Tag Status Monitor stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self._running:
            try:
                current_time = time.time()
                newly_inactive = []
                
                with self._lock:
                    for tag_id, last_timestamp in self.tag_last_seen.items():
                        time_since_last_update = current_time - last_timestamp
                        
                        # Check if tag has timed out
                        if time_since_last_update > self.timeout_seconds:
                            if tag_id not in self.inactive_tags:
                                self.inactive_tags.add(tag_id)
                                newly_inactive.append(tag_id)
                
                # Log newly inactive tags outside the lock
                for tag_id in newly_inactive:
                    time_diff = current_time - self.tag_last_seen.get(tag_id, 0)
                    logger.warning(f"Tag {tag_id} marked as inactive (no data for {time_diff:.1f}s)")
                
                # Sleep for a reasonable check interval
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in tag status monitor loop: {e}")
                time.sleep(5)
    
    def get_stats(self) -> dict:
        """Get monitoring statistics"""
        with self._lock:
            return {
                'total_tags_monitored': len(self.tag_last_seen),
                'inactive_tags_count': len(self.inactive_tags),
                'active_tags_count': len(self.tag_last_seen) - len(self.inactive_tags),
                'timeout_seconds': self.timeout_seconds
            }


# Global instance
_tag_status_monitor: TagStatusMonitor = None

def get_tag_status_monitor() -> TagStatusMonitor:
    """Get global tag status monitor instance"""
    global _tag_status_monitor
    if _tag_status_monitor is None:
        _tag_status_monitor = TagStatusMonitor(timeout_seconds=30)
        _tag_status_monitor.start()
    return _tag_status_monitor

def shutdown_tag_status_monitor():
    """Shutdown global tag status monitor"""
    global _tag_status_monitor
    if _tag_status_monitor:
        _tag_status_monitor.stop()
        _tag_status_monitor = None