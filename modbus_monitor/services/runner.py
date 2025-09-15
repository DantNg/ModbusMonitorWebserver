from __future__ import annotations
from queue import Queue
import threading

from modbus_monitor.services.common import LatestCache
from modbus_monitor.services.db_writer import DBWriter
from modbus_monitor.services.modbus_service import ModbusService
from modbus_monitor.services.alarm_service import AlarmService
from modbus_monitor.services.datalogger_service import DataLoggerService

# Singleton đơn giản
from typing import Optional

_cache: Optional[LatestCache] = None
_dbq: Optional[Queue] = None
_pushq: Optional[Queue] = None
_writer: Optional[DBWriter] = None
_modbus: Optional[ModbusService] = None
_alarm: Optional[AlarmService] = None
_logger: Optional[DataLoggerService] = None

_started = False
_lock = threading.RLock()

def start_services():
    global _started, _cache, _dbq, _pushq, _writer, _modbus, _alarm, _logger
    with _lock:
        if _started:
            print("Services already started, skipping...")
            return
        
        # Check if we're in the main process to avoid COM port conflicts
        import multiprocessing
        current_process = multiprocessing.current_process()
        if current_process.name != 'MainProcess':
            print(f"Skipping services start in worker process: {current_process.name}")
            return
            
        print("🚀 Starting services in main process...")
        _cache = LatestCache()
        _dbq = Queue(maxsize=50000)
        _pushq = Queue(maxsize=50000)
        _writer = DBWriter(_dbq)
        _modbus = ModbusService(_dbq, _pushq, _cache)
        _alarm = AlarmService(_cache)
        _logger = DataLoggerService(_cache)
        
        _writer.start()
        _modbus.start()
        _alarm.start()
        _logger.start()
        _started = True
        print("✅ All services started successfully in main process")

def stop_services():
    global _started, _cache, _dbq, _pushq, _writer, _modbus, _alarm, _logger
    with _lock:
        if not _started:
            return
        print("🛑 Stopping services...")
        try:
            if _modbus: 
                _modbus.stop()
                print("   ✓ Modbus service stopped")
        finally:
            if _alarm: 
                _alarm.stop()
                print("   ✓ Alarm service stopped")
            if _logger: 
                _logger.stop()
                print("   ✓ DataLogger service stopped")
            if _writer: 
                _writer.stop()
                print("   ✓ DB Writer stopped")
        _started = False
        print("✅ All services stopped")

def restart_services():
    """Restart all services to pick up configuration changes."""
    global _lock
    with _lock:
        # Thay vì restart toàn bộ, chỉ reload configs
        if _modbus:
            _modbus.reload_configs()
        print("Services configuration reloaded")

def reload_device_configs():
    """Reload device configs without full restart"""
    global _modbus, _lock
    with _lock:
        if _modbus:
            _modbus.reload_configs()
            print("Device configurations reloaded")
        else:
            print("Modbus service not started")

def write_tag_value(tag_id: int, value: float) -> bool:
    """
    Global function to write a value to a tag.
    Returns True if successful, False otherwise.
    """
    global _modbus
    if not _modbus:
        print("Modbus service not started")
        return False
    return _modbus.write_tag_value(tag_id, value)

def get_modbus_service():
    """Get the ModbusService instance for direct access."""
    return _modbus

def services_status():
    """Check if services are running."""
    global _modbus
    status = {
        "running": _started
    }
    
    if _modbus:
        status["modbus_stats"] = _modbus.get_stats()
    
    return status
