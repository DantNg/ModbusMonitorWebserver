from __future__ import annotations
from queue import Queue
import threading

from modbus_monitor.services.common import LatestCache
from modbus_monitor.services.db_writer import DBWriter
from modbus_monitor.services.modbus_service import ModbusService
from modbus_monitor.services.alarm_service import AlarmService
from modbus_monitor.services.datalogger_service import DataLoggerService

# Singleton đơn giản
_cache: LatestCache | None = None
_dbq: Queue | None = None
_writer: DBWriter | None = None
_modbus: ModbusService | None = None
_alarm: AlarmService | None = None
_logger: DataLoggerService | None = None
_started = False
_lock = threading.RLock()

def start_services():
    global _started, _cache, _dbq, _writer, _modbus, _alarm, _logger
    with _lock:
        if _started:
            return
        _cache = LatestCache()
        _dbq = Queue(maxsize=5000)
        _writer = DBWriter(_dbq); _writer.start()
        _modbus = ModbusService(_dbq, _cache); _modbus.start()
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

# tiện cho UI kiểm tra nhanh
def services_status():
    return {
        "running": _started
    }
