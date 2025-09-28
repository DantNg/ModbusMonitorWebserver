"""
Device Status Synchronization Service

Đồng bộ trạng thái device từ config_cache vào database định kỳ
"""
import threading
import time
from typing import Optional


class DeviceSyncService(threading.Thread):
    """Service đồng bộ device status từ config_cache vào database"""
    
    def __init__(self, sync_interval: int = 30):
        """
        Args:
            sync_interval: Thời gian đồng bộ (giây), mặc định 30s
        """
        super().__init__(daemon=True, name="DeviceSyncService")
        self.sync_interval = sync_interval
        self._stop_event = threading.Event()
        self._last_sync_time = 0
        self._sync_count = 0
        self._error_count = 0
        
    def run(self):
        """Main loop đồng bộ device status"""
        print(f"🔄 DeviceSyncService started - syncing to MySQL every {self.sync_interval}s")
        
        # Thực hiện sync ngay khi bắt đầu
        try:
            print("🚀 Initial sync on startup...")
            updated_count = self._sync_device_status()
            print(f"✅ Initial sync completed: {updated_count} devices updated")
        except Exception as e:
            print(f"❌ Initial sync failed: {e}")
        
        while not self._stop_event.is_set():
            try:
                # print(f"⏰ Starting scheduled sync to MySQL... (interval: {self.sync_interval}s)")
                start_time = time.time()
                
                # Thực hiện đồng bộ
                updated_count = self._sync_device_status()
                
                # Cập nhật stats
                self._last_sync_time = time.time()
                self._sync_count += 1
                
                sync_duration = self._last_sync_time - start_time
                
            except Exception as e:
                self._error_count += 1
                print(f"❌ DeviceSync error #{self._error_count}: {e}")
            
            # Chờ interval hoặc stop signal
            # print(f"😴 Waiting {self.sync_interval}s until next sync...")
            self._stop_event.wait(self.sync_interval)
            
        print("🛑 DeviceSyncService stopped")
    
    def _sync_device_status(self) -> int:
        """Thực hiện đồng bộ device status từ config_cache lên MySQL"""
        try:
            from modbus_monitor.database.db import sync_device_status_to_mysql
            result = sync_device_status_to_mysql()
            
            if result.get("success"):
                return result.get("updated_count", 0)
            else:
                print(f"❌ MySQL sync failed: {result.get('error', 'Unknown error')}")
                return 0
                
        except ImportError:
            print("❌ Cannot import MySQL sync function")
            return 0
        except Exception as e:
            print(f"❌ MySQL sync failed: {e}")
            return 0
    
    def stop(self):
        """Dừng service"""
        print("🛑 Stopping DeviceSyncService...")
        self._stop_event.set()
        
    def force_sync(self) -> dict:
        """Force đồng bộ ngay lập tức và trả về kết quả chi tiết"""
        print("🔄 Force syncing device status to MySQL...")
        try:
            from modbus_monitor.database.db import sync_device_status_to_mysql
            return sync_device_status_to_mysql()
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "updated_count": 0
            }
    
    def get_stats(self) -> dict:
        """Lấy thống kê service"""
        return {
            "sync_interval": self.sync_interval,
            "last_sync_time": self._last_sync_time,
            "sync_count": self._sync_count,
            "error_count": self._error_count,
            "is_alive": self.is_alive()
        }


# Singleton instance
_device_sync_service: Optional[DeviceSyncService] = None
_sync_lock = threading.RLock()

def get_device_sync_service() -> Optional[DeviceSyncService]:
    """Lấy instance của DeviceSyncService"""
    return _device_sync_service

def start_device_sync_service(sync_interval: int = 30):
    """Khởi động DeviceSyncService"""
    global _device_sync_service, _sync_lock
    
    with _sync_lock:
        if _device_sync_service is not None and _device_sync_service.is_alive():
            print("DeviceSyncService already running")
            return _device_sync_service
        
        _device_sync_service = DeviceSyncService(sync_interval)
        _device_sync_service.start()
        return _device_sync_service

def stop_device_sync_service():
    """Dừng DeviceSyncService"""
    global _device_sync_service, _sync_lock
    
    with _sync_lock:
        if _device_sync_service is not None:
            _device_sync_service.stop()
            _device_sync_service = None

def force_device_sync() -> dict:
    """Force đồng bộ device status ngay lập tức"""
    service = get_device_sync_service()
    if service:
        return service.force_sync()
    else:
        # Fallback: chạy sync trực tiếp
        try:
            from modbus_monitor.database.db import sync_device_status_to_mysql
            return sync_device_status_to_mysql()
        except Exception as e:
            return {
                "success": False,
                "error": f"Force sync failed: {e}",
                "updated_count": 0
            }