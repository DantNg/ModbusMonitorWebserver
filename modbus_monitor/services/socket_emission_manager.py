"""
Socket Emission Manager để xử lý batch socket emission
Giảm xung đột và tăng performance
"""
import threading
import time
from queue import Queue, Empty
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class SocketMessage:
    """Message để gửi qua socket"""
    message_type: str  # "device_update", "device_disconnect", etc.
    device_id: str
    device_name: str
    unit: int
    data: Dict[str, Any]
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()

class SocketEmissionManager:
    """
    Manager để xử lý socket emission batch và async
    - Gom nhiều messages thành batch để giảm overhead
    - Tránh xung đột giữa các threads
    - Non-blocking queue để không làm chậm Modbus threads
    """
    
    def __init__(self, max_batch_size: int = 20, batch_timeout: float = 0.1, queue_size: int = 10000):
        self._queue = Queue(maxsize=queue_size)
        self._max_batch_size = max_batch_size
        self._batch_timeout = batch_timeout
        self._worker_thread: Optional[threading.Thread] = None
        self._shutdown = False
        
        # Cache cho subdashboard mappings
        self._subdash_cache: Dict[int, set] = {}
        self._subdash_cache_time = 0.0
        self._subdash_cache_interval = 10.0  # Reload mỗi 10s (reduced from 60s)
        
        self._start_worker()
    
    def _start_worker(self):
        """Khởi động worker thread"""
        self._worker_thread = threading.Thread(
            target=self._emission_worker,
            daemon=True,
            name="SocketEmissionWorker"
        )
        self._worker_thread.start()
        print("Socket emission worker started")
    
    def emit_device_update(self, device_id: str, device_name: str, unit: int, 
                          ok: bool, tags: List[Dict] = None, seq: int = 0, 
                          latency_ms: int = 0, error: str = None, status: str = None):
        """Emit device update message"""
        data = {
            "ok": ok,
            "seq": seq,
            "ts": datetime.now().strftime("%H:%M:%S")
        }
        
        if ok and tags:
            data["tags"] = tags
            data["latency_ms"] = latency_ms
            data["tag_count"] = len(tags)
        else:
            data["error"] = error or "Connection failed"
            data["status"] = status or "disconnected"
        
        message = SocketMessage(
            message_type="device_update",
            device_id=device_id,
            device_name=device_name,
            unit=unit,
            data=data
        )
        
        self._enqueue_message(message)
    
    def _enqueue_message(self, message: SocketMessage):
        """Thêm message vào queue (non-blocking)"""
        try:
            self._queue.put_nowait(message)
        except:
            # Queue full, skip message để không block Modbus thread
            pass
    
    def _emission_worker(self):
        """Worker thread xử lý socket emission"""
        batch_buffer = []
        last_flush = time.time()
        
        while not self._shutdown:
            try:
                # Collect messages với timeout ngắn
                try:
                    message = self._queue.get(timeout=0.05)
                    batch_buffer.append(message)
                except Empty:
                    pass
                
                current_time = time.time()
                
                # Flush batch khi đủ điều kiện
                should_flush = (
                    batch_buffer and (
                        len(batch_buffer) >= self._max_batch_size or
                        current_time - last_flush >= self._batch_timeout
                    )
                )
                
                if should_flush:
                    self._process_batch(batch_buffer)
                    batch_buffer.clear()
                    last_flush = current_time
                    
            except Exception as e:
                print(f"Socket emission worker error: {e}")
                time.sleep(0.1)
    
    def _process_batch(self, messages: List[SocketMessage]):
        """Xử lý batch messages"""
        try:
            # Group messages by device để merge updates
            device_updates = {}
            
            for msg in messages:
                if msg.message_type == "device_update":
                    device_id = msg.device_id
                    
                    if device_id not in device_updates:
                        device_updates[device_id] = {
                            "device_id": msg.device_id,
                            "device_name": msg.device_name,
                            "unit": msg.unit,
                            "ok": msg.data["ok"],
                            "seq": msg.data["seq"],
                            "ts": msg.data["ts"],
                            "tags": [],
                            "latency_ms": msg.data.get("latency_ms", 0)
                        }
                        
                        # Copy other fields
                        if not msg.data["ok"]:
                            device_updates[device_id]["error"] = msg.data.get("error")
                            device_updates[device_id]["status"] = msg.data.get("status")
                    
                    # Merge tags from multiple messages
                    if msg.data["ok"] and "tags" in msg.data:
                        device_updates[device_id]["tags"].extend(msg.data["tags"])
                        # Update latest latency
                        device_updates[device_id]["latency_ms"] = msg.data.get("latency_ms", 0)
            
            # Emit merged updates
            self._emit_device_updates(device_updates)
            
        except Exception as e:
            print(f"Error processing socket batch: {e}")
    
    def _emit_device_updates(self, device_updates: Dict[str, Dict]):
        """Emit device updates tới các rooms"""
        try:
            from modbus_monitor.extensions import socketio
            
            for device_id, update in device_updates.items():
                # Emit to device room
                socketio.emit("modbus_update", update, room=f"dashboard_device_{device_id}")
                
                # Emit to relevant subdashboards
                if update["ok"] and update["tags"]:
                    self._emit_to_subdashboards(socketio, update)
                    
        except Exception as e:
            print(f"Socket emission error: {e}")
    
    def _emit_to_subdashboards(self, socketio, update: Dict):
        """Emit update tới các subdashboards có liên quan"""
        try:
            # Update subdashboard cache nếu cần
            self._update_subdash_cache_if_needed()
            
            if not self._subdash_cache:
                return
            
            # Get tag IDs from update
            tag_ids = set(tag["id"] for tag in update["tags"])
            
            # Find relevant subdashboards và emit
            for subdash_id, subdash_tag_set in self._subdash_cache.items():
                # Filter tags relevant to this subdashboard
                relevant_tags = [
                    tag for tag in update["tags"] 
                    if tag["id"] in subdash_tag_set
                ]
                
                if relevant_tags:
                    # Create subdashboard-specific update
                    subdash_update = update.copy()
                    subdash_update["tags"] = relevant_tags
                    subdash_update["tag_count"] = len(relevant_tags)
                    
                    socketio.emit("modbus_update", subdash_update, 
                                room=f"subdashboard_{subdash_id}")
                    
        except Exception as e:
            print(f"Subdashboard emission error: {e}")
    
    def _update_subdash_cache_if_needed(self):
        """Update subdashboard cache nếu cần"""
        current_time = time.time()
        
        if current_time - self._subdash_cache_time > self._subdash_cache_interval:
            try:
                from modbus_monitor.database import db as dbsync
                
                self._subdash_cache.clear()
                subdashboards = dbsync.list_subdashboards() or []
                
                for subdash in subdashboards:
                    subdash_id = subdash['id']
                    tag_ids = [
                        t['id'] for t in dbsync.get_subdashboard_tags(subdash_id) or []
                    ]
                    self._subdash_cache[subdash_id] = set(tag_ids)
                
                self._subdash_cache_time = current_time
                # print(f"Updated subdashboard cache: {len(self._subdash_cache)} subdashboards")
                
            except Exception as e:
                print(f"Error updating subdashboard cache: {e}")
    
    def force_refresh_subdash_cache(self):
        """Force immediate refresh of subdashboard cache"""
        try:
            from modbus_monitor.database import db as dbsync
            
            self._subdash_cache.clear()
            subdashboards = dbsync.list_subdashboards() or []
            
            for subdash in subdashboards:
                subdash_id = subdash['id']
                tag_ids = [
                    t['id'] for t in dbsync.get_subdashboard_tags(subdash_id) or []
                ]
                self._subdash_cache[subdash_id] = set(tag_ids)
            
            self._subdash_cache_time = time.time()
            print(f"Force refreshed subdashboard cache: {len(self._subdash_cache)} subdashboards")
            
        except Exception as e:
            print(f"Error force refreshing subdashboard cache: {e}")
    
    def get_stats(self) -> Dict:
        """Lấy thống kê emission manager"""
        return {
            "queue_size": self._queue.qsize(),
            "subdash_cache_size": len(self._subdash_cache),
            "last_subdash_update": self._subdash_cache_time,
            "worker_active": self._worker_thread.is_alive() if self._worker_thread else False
        }
    
    def shutdown(self):
        """Shutdown emission manager"""
        print("Shutting down socket emission manager...")
        self._shutdown = True
        
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        
        print("Socket emission manager shut down")

# Global emission manager instance
_emission_manager: Optional[SocketEmissionManager] = None
_manager_lock = threading.Lock()

def get_emission_manager() -> SocketEmissionManager:
    """Lấy global emission manager (singleton)"""
    global _emission_manager
    if _emission_manager is None:
        with _manager_lock:
            if _emission_manager is None:
                _emission_manager = SocketEmissionManager()
    return _emission_manager

def shutdown_emission_manager():
    """Shutdown global emission manager"""
    global _emission_manager
    if _emission_manager:
        _emission_manager.shutdown()
        _emission_manager = None
