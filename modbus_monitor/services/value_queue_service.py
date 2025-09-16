"""
Value Queue Service
Quản lý queue buffer giữa Modbus producer và các consumers (parser, datalogger)
"""
import threading
import queue
import time
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

@dataclass
class RawModbusValue:
    """Container cho raw modbus value chưa được parse"""
    device_id: int
    tag_id: int
    tag_name: str
    function_code: int
    address: int
    raw_value: Any  # Raw value từ modbus
    timestamp: float
    data_type: str
    scale: float
    offset: float
    unit: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for easy processing"""
        return {
            'device_id': self.device_id,
            'tag_id': self.tag_id,
            'tag_name': self.tag_name,
            'function_code': self.function_code,
            'address': self.address,
            'raw_value': self.raw_value,
            'timestamp': self.timestamp,
            'data_type': self.data_type,
            'scale': self.scale,
            'offset': self.offset,
            'unit': self.unit
        }

class ValueQueueService:
    """
    Service quản lý queue cho raw modbus values
    Sử dụng singleton pattern để đảm bảo chỉ có 1 instance
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
            
        self._initialized = True
        
        # Main queue cho raw values
        self.raw_value_queue = queue.Queue(maxsize=10000)  # Buffer 10k values
        
        # Separate queues cho từng consumer type
        self.parser_queue = queue.Queue(maxsize=5000)      # UI parser
        self.datalogger_queue = queue.Queue(maxsize=5000)  # DataLogger
        
        # Stats tracking
        self.stats = {
            'total_enqueued': 0,
            'total_parsed': 0,
            'total_logged': 0,
            'queue_full_errors': 0,
            'start_time': time.time()
        }
        
        # Control flags
        self.running = True
        self._distributor_thread = None
        
        # Locks
        self.stats_lock = threading.Lock()
        
        logger.info("ValueQueueService initialized")
    
    def start_distributor(self):
        """Khởi động thread phân phối values từ main queue sang consumer queues"""
        if self._distributor_thread is None or not self._distributor_thread.is_alive():
            self._distributor_thread = threading.Thread(
                target=self._distribute_values,
                daemon=True,
                name="ValueDistributor"
            )
            self._distributor_thread.start()
            logger.info("Value distributor thread started")
    
    def stop(self):
        """Dừng service"""
        self.running = False
        if self._distributor_thread and self._distributor_thread.is_alive():
            self._distributor_thread.join(timeout=2.0)
        logger.info("ValueQueueService stopped")
    
    def enqueue_raw_value(self, raw_value: RawModbusValue) -> bool:
        """
        Thêm raw value vào main queue
        Returns: True nếu thành công, False nếu queue đầy
        """
        try:
            self.raw_value_queue.put_nowait(raw_value)
            
            with self.stats_lock:
                self.stats['total_enqueued'] += 1
            
            return True
            
        except queue.Full:
            with self.stats_lock:
                self.stats['queue_full_errors'] += 1
            
            logger.warning(f"Raw value queue is full, dropping value for device {raw_value.device_id}")
            return False
    
    def enqueue_raw_values_batch(self, raw_values: List[RawModbusValue]) -> int:
        """
        Thêm batch raw values
        Returns: Số lượng values được enqueue thành công
        """
        success_count = 0
        for raw_value in raw_values:
            if self.enqueue_raw_value(raw_value):
                success_count += 1
        
        return success_count
    
    def get_parser_value(self, timeout: float = 1.0) -> Optional[RawModbusValue]:
        """Lấy value từ parser queue cho UI processing"""
        try:
            return self.parser_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def get_parser_values_batch(self, max_count: int = 50, timeout: float = 0.1) -> List[RawModbusValue]:
        """Lấy batch values từ parser queue"""
        values = []
        
        try:
            # Lấy value đầu tiên với timeout
            first_value = self.parser_queue.get(timeout=timeout)
            values.append(first_value)
            
            # Lấy thêm các values khác không đợi
            for _ in range(max_count - 1):
                try:
                    value = self.parser_queue.get_nowait()
                    values.append(value)
                except queue.Empty:
                    break
                    
        except queue.Empty:
            pass
        
        return values
    
    def get_datalogger_value(self, timeout: float = 1.0) -> Optional[RawModbusValue]:
        """Lấy value từ datalogger queue"""
        try:
            return self.datalogger_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def get_datalogger_values_batch(self, max_count: int = 100, timeout: float = 0.1) -> List[RawModbusValue]:
        """Lấy batch values từ datalogger queue"""
        values = []
        
        try:
            # Lấy value đầu tiên với timeout
            first_value = self.datalogger_queue.get(timeout=timeout)
            values.append(first_value)
            
            # Lấy thêm các values khác không đợi
            for _ in range(max_count - 1):
                try:
                    value = self.datalogger_queue.get_nowait()
                    values.append(value)
                except queue.Empty:
                    break
                    
        except queue.Empty:
            pass
        
        return values
    
    def _distribute_values(self):
        """Thread function phân phối values từ main queue sang consumer queues"""
        logger.info("Value distributor started")
        
        while self.running:
            try:
                # Lấy raw value từ main queue
                raw_value = self.raw_value_queue.get(timeout=0.5)
                
                # Phân phối sang cả 2 consumer queues
                distributed = 0
                
                # Gửi cho parser (UI)
                try:
                    self.parser_queue.put_nowait(raw_value)
                    distributed += 1
                except queue.Full:
                    logger.warning("Parser queue is full, dropping value")
                
                # Gửi cho datalogger  
                try:
                    self.datalogger_queue.put_nowait(raw_value)
                    distributed += 1
                except queue.Full:
                    logger.warning("DataLogger queue is full, dropping value")
                
                # Mark task done
                self.raw_value_queue.task_done()
                
                if distributed == 0:
                    logger.warning("Failed to distribute value to any consumer")
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in value distributor: {e}")
        
        logger.info("Value distributor stopped")
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Lấy thống kê queue"""
        with self.stats_lock:
            runtime = time.time() - self.stats['start_time']
            
            return {
                'runtime_seconds': runtime,
                'raw_queue_size': self.raw_value_queue.qsize(),
                'parser_queue_size': self.parser_queue.qsize(),
                'datalogger_queue_size': self.datalogger_queue.qsize(),
                'total_enqueued': self.stats['total_enqueued'],
                'total_parsed': self.stats['total_parsed'],
                'total_logged': self.stats['total_logged'],
                'queue_full_errors': self.stats['queue_full_errors'],
                'enqueue_rate_per_sec': self.stats['total_enqueued'] / runtime if runtime > 0 else 0,
                'parse_rate_per_sec': self.stats['total_parsed'] / runtime if runtime > 0 else 0,
                'log_rate_per_sec': self.stats['total_logged'] / runtime if runtime > 0 else 0
            }
    
    def mark_parsed(self, count: int = 1):
        """Đánh dấu số lượng values đã được parse"""
        with self.stats_lock:
            self.stats['total_parsed'] += count
    
    def mark_logged(self, count: int = 1):
        """Đánh dấu số lượng values đã được log"""
        with self.stats_lock:
            self.stats['total_logged'] += count
    
    def clear_queues(self):
        """Xóa tất cả queues (dùng khi restart)"""
        try:
            while not self.raw_value_queue.empty():
                self.raw_value_queue.get_nowait()
                
            while not self.parser_queue.empty():
                self.parser_queue.get_nowait()
                
            while not self.datalogger_queue.empty():
                self.datalogger_queue.get_nowait()
                
            logger.info("All queues cleared")
            
        except Exception as e:
            logger.error(f"Error clearing queues: {e}")

# Global instance
value_queue_service = ValueQueueService()
