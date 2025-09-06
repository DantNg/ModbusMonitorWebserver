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
_writer: Optional[DBWriter] = None
_modbus: Optional[ModbusService] = None
_alarm: Optional[AlarmService] = None
_logger: Optional[DataLoggerService] = None

_started = False
_lock = threading.RLock()

def start_services():
    global _started, _cache, _dbq,_pushq, _writer, _modbus, _alarm, _logger
    with _lock:
        if _started:
            return
        _cache = LatestCache()
        _dbq = Queue(maxsize=5000)
        _pushq = Queue(maxsize=5000)
        _writer = DBWriter(_dbq); _writer.start()
        _modbus = ModbusService(_dbq, _pushq,_cache); _modbus.start()
        _alarm = AlarmService(_cache); _alarm.start()
        _logger = DataLoggerService(_cache); _logger.start()
        _started = True

def stop_services():
    global _started, _writer, _modbus, _alarm, _logger
    with _lock:
        if not _started:
            return
        try:
            if _modbus: _modbus.stop()
        finally:
            if _alarm: _alarm.stop()
            if _logger: _logger.stop()
            if _writer: _writer.stop()
        _started = False

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

# tiện cho UI kiểm tra nhanh
def services_status():
    return {
        "running": _started
    }
