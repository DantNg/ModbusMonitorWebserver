"""
RTU Connection Pool để quản lý kết nối Modbus RTU chia sẻ
Giải quyết vấn đề mở/đóng COM port liên tục gây lỗi
"""
import threading
import time
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass
from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ConnectionException, ModbusIOException
import logging

logger = logging.getLogger(__name__)

@dataclass
class RTUConnectionConfig:
    """Cấu hình cho một kết nối RTU"""
    serial_port: str
    baudrate: int = 9600
    bytesize: int = 8
    parity: str = "N"
    stopbits: int = 1
    timeout: float = 1
    
    def __hash__(self):
        """Tạo hash key để làm key cho dictionary"""
        return hash((self.serial_port, self.baudrate, self.bytesize, self.parity, self.stopbits))
    
    def __eq__(self, other):
        """So sánh 2 config có giống nhau không"""
        if not isinstance(other, RTUConnectionConfig):
            return False
        return (self.serial_port == other.serial_port and 
                self.baudrate == other.baudrate and
                self.bytesize == other.bytesize and 
                self.parity == other.parity and
                self.stopbits == other.stopbits)

@dataclass
class RTUConnectionEntry:
    """Entry trong connection pool"""
    client: ModbusSerialClient
    config: RTUConnectionConfig
    ref_count: int = 0
    last_used: float = 0.0
    is_connected: bool = False
    lock: threading.RLock = None
    
    def __post_init__(self):
        if self.lock is None:
            self.lock = threading.RLock()

class RTUConnectionPool:
    """
    Pool quản lý các kết nối Modbus RTU được chia sẻ
    - Mỗi COM port chỉ mở 1 lần và chia sẻ cho nhiều device
    - Tự động đóng kết nối khi không còn sử dụng
    - Thread-safe với locking
    """
    
    def __init__(self, cleanup_interval: float = 30.0, idle_timeout: float = 60.0):
        self._connections: Dict[RTUConnectionConfig, RTUConnectionEntry] = {}
        self._pool_lock = threading.RLock()
        self._cleanup_interval = cleanup_interval
        self._idle_timeout = idle_timeout
        self._cleanup_thread = None
        self._shutdown = False
        
        # Start cleanup thread
        self._start_cleanup_thread()
    
    def _start_cleanup_thread(self):
        """Khởi động thread cleanup để đóng các kết nối idle"""
        def cleanup_worker():
            while not self._shutdown:
                try:
                    self._cleanup_idle_connections()
                    time.sleep(self._cleanup_interval)
                except Exception as e:
                    logger.error(f"Error in cleanup thread: {e}")
                    time.sleep(5)
        
        self._cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True, name="RTU-Cleanup")
        self._cleanup_thread.start()
    
    def get_connection(self, config: RTUConnectionConfig) -> Optional[RTUConnectionEntry]:
        """
        Lấy kết nối từ pool. Tạo mới nếu chưa có.
        Returns: RTUConnectionEntry nếu thành công, None nếu thất bại
        """
        with self._pool_lock:
            # Tìm kết nối existing
            entry = self._connections.get(config)
            
            if entry is None:
                # Tạo kết nối mới
                entry = self._create_new_connection(config)
                if entry is None:
                    return None
                self._connections[config] = entry
            
            # Increment reference count
            entry.ref_count += 1
            entry.last_used = time.time()
            
            # Ensure connection is active
            if not entry.is_connected:
                if not self._ensure_connected(entry):
                    entry.ref_count -= 1
                    return None
            
            logger.debug(f"Acquired RTU connection for {config.serial_port}, ref_count: {entry.ref_count}")
            return entry
    
    def release_connection(self, config: RTUConnectionConfig):
        """
        Giải phóng kết nối (giảm ref count)
        """
        with self._pool_lock:
            entry = self._connections.get(config)
            if entry and entry.ref_count > 0:
                entry.ref_count -= 1
                entry.last_used = time.time()
                logger.debug(f"Released RTU connection for {config.serial_port}, ref_count: {entry.ref_count}")
    
    def _create_new_connection(self, config: RTUConnectionConfig) -> Optional[RTUConnectionEntry]:
        """Tạo kết nối RTU mới"""
        try:
            client = ModbusSerialClient(
                port=config.serial_port,
                baudrate=config.baudrate,
                bytesize=config.bytesize,
                parity=config.parity,
                stopbits=config.stopbits,
                timeout=config.timeout
            )
            
            entry = RTUConnectionEntry(
                client=client,
                config=config,
                ref_count=0,
                last_used=time.time(),
                is_connected=False
            )
            
            logger.info(f"Created new RTU connection for {config.serial_port}")
            return entry
            
        except Exception as e:
            logger.error(f"Failed to create RTU connection for {config.serial_port}: {e}")
            return None
    
    def _ensure_connected(self, entry: RTUConnectionEntry) -> bool:
        """Đảm bảo kết nối đang active"""
        with entry.lock:
            if entry.is_connected:
                # Test connection
                if self._test_connection(entry):
                    return True
                else:
                    logger.warning(f"RTU connection {entry.config.serial_port} lost, reconnecting...")
                    entry.is_connected = False
            
            # Reconnect
            try:
                if entry.client.connect():
                    entry.is_connected = True
                    logger.info(f"RTU connection {entry.config.serial_port} established")
                    return True
                else:
                    logger.error(f"Failed to connect RTU {entry.config.serial_port}")
                    return False
            except Exception as e:
                logger.error(f"RTU connection error for {entry.config.serial_port}: {e}")
                return False
    
    def _test_connection(self, entry: RTUConnectionEntry) -> bool:
        """Test xem kết nối còn hoạt động không"""
        try:
            # Thử đọc 1 register để test
            result = entry.client.read_holding_registers(0, 1, slave=1)
            # Không quan trọng kết quả, chỉ cần không bị exception
            return True
        except Exception:
            return False
    
    def _cleanup_idle_connections(self):
        """Đóng các kết nối idle"""
        current_time = time.time()
        to_remove = []
        
        with self._pool_lock:
            for config, entry in self._connections.items():
                if (entry.ref_count == 0 and 
                    current_time - entry.last_used > self._idle_timeout):
                    to_remove.append(config)
            
            for config in to_remove:
                entry = self._connections.pop(config)
                self._close_connection(entry)
                logger.info(f"Closed idle RTU connection for {config.serial_port}")
    
    def _close_connection(self, entry: RTUConnectionEntry):
        """Đóng một kết nối"""
        try:
            with entry.lock:
                if entry.client:
                    entry.client.close()
                entry.is_connected = False
        except Exception as e:
            logger.error(f"Error closing RTU connection: {e}")
    
    def shutdown(self):
        """Shutdown pool và đóng tất cả kết nối"""
        self._shutdown = True
        
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)
        
        with self._pool_lock:
            for entry in self._connections.values():
                self._close_connection(entry)
            self._connections.clear()
        
        logger.info("RTU Connection Pool shut down")
    
    def get_stats(self) -> Dict:
        """Lấy thống kê pool"""
        with self._pool_lock:
            return {
                "total_connections": len(self._connections),
                "active_connections": sum(1 for e in self._connections.values() if e.ref_count > 0),
                "connections": [
                    {
                        "port": config.serial_port,
                        "baudrate": config.baudrate,
                        "ref_count": entry.ref_count,
                        "is_connected": entry.is_connected,
                        "last_used": entry.last_used
                    }
                    for config, entry in self._connections.items()
                ]
            }

# Global pool instance
_rtu_pool: Optional[RTUConnectionPool] = None
_pool_lock = threading.Lock()

def get_rtu_pool() -> RTUConnectionPool:
    """Lấy global RTU connection pool (singleton)"""
    global _rtu_pool
    if _rtu_pool is None:
        with _pool_lock:
            if _rtu_pool is None:
                _rtu_pool = RTUConnectionPool()
    return _rtu_pool

def shutdown_rtu_pool():
    """Shutdown global pool"""
    global _rtu_pool
    if _rtu_pool:
        _rtu_pool.shutdown()
        _rtu_pool = None
