from __future__ import annotations
from queue import Queue
import threading

from modbus_monitor.services.common import LatestCache
from modbus_monitor.services.db_writer import DBWriter
from modbus_monitor.services.modbus_service import ModbusService
from modbus_monitor.services.alarm_service import AlarmService
from modbus_monitor.services.datalogger_service import DataLoggerService
from modbus_monitor.services.value_parser_service import ValueParserService

# Singleton đơn giản
from typing import Optional

_cache: Optional[LatestCache] = None
_dbq: Optional[Queue] = None
_pushq: Optional[Queue] = None
_writer: Optional[DBWriter] = None
_modbus: Optional[ModbusService] = None
_alarm: Optional[AlarmService] = None
_logger: Optional[DataLoggerService] = None
_parser: Optional[ValueParserService] = None

_started = False
_lock = threading.RLock()

def start_services():
    global _started, _cache, _dbq, _pushq, _writer, _modbus, _alarm, _logger, _parser
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
            
        print("🚀 Starting services with new queue-based architecture...")
        
        # Initialize shared components
        _cache = LatestCache()
        
        # Legacy DB queue for compatibility (optional - could be removed)
        _dbq = Queue(maxsize=50000)
        _pushq = Queue(maxsize=50000)  # May not be needed anymore
        
        # Initialize new architecture services
        print("📦 Starting ModbusService (Producer)...")
        _modbus = ModbusService()  # No longer needs queues/cache
        
        print("🔄 Starting ValueParserService (Consumer - UI)...")
        _parser = ValueParserService(_cache)
        
        print("📊 Starting DataLoggerService (Consumer - DB)...")
        _logger = DataLoggerService(_cache)
        
        print("⚠️ Starting AlarmService...")
        _alarm = AlarmService(_cache)
        
        # Legacy DB writer (may not be needed with new architecture)
        print("💾 Starting DBWriter (Legacy)...")
        _writer = DBWriter(_dbq)
        
        # Start all services
        _writer.start()
        _modbus.start()      # Starts Modbus readers (producers)
        _parser.start()      # Starts value parser (consumer)
        _logger.start()      # Starts datalogger (consumer) 
        _alarm.start()
        
        _started = True
        print("✅ All services started successfully with queue-based architecture!")
        print("🏗️ Architecture: Modbus(Producer) → Queue → Parser(UI) + DataLogger(DB)")

def stop_services():
    global _started, _cache, _dbq, _pushq, _writer, _modbus, _alarm, _logger, _parser
    with _lock:
        if not _started:
            return
        print("🛑 Stopping services...")
        try:
            if _modbus: 
                _modbus.stop()
                print("   ✓ Modbus service (Producer) stopped")
        finally:
            pass
        
        try:
            if _parser:
                _parser.stop()
                print("   ✓ Value parser service (Consumer-UI) stopped")
        finally:
            pass
            
        try:
            if _alarm: 
                _alarm.stop()
                print("   ✓ Alarm service stopped")
        finally:
            pass
            
        try:
            if _logger: 
                _logger.stop()
                print("   ✓ DataLogger service (Consumer-DB) stopped")
        finally:
            pass
            
        try:
            if _writer: 
                _writer.stop()
                print("   ✓ DB writer (Legacy) stopped")
        finally:
            pass

        _started = False
        print("✅ All services stopped")

def restart_services():
    """Restart all services"""
    print("🔄 Restarting services...")
    stop_services()
    
    # Clear global references
    global _cache, _dbq, _pushq, _writer, _modbus, _alarm, _logger, _parser
    _cache = None
    _dbq = None  
    _pushq = None
    _writer = None
    _modbus = None
    _alarm = None
    _logger = None
    _parser = None
    
    start_services()
    print("✅ Services restarted with new architecture")

def get_modbus_service():
    """Get the ModbusService instance for direct access."""
    return _modbus

def get_value_parser_service():
    """Get the ValueParserService instance for stats."""
    return _parser

def get_datalogger_service():
    """Get the DataLoggerService instance for stats."""
    return _logger

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

def services_status():
    """Check if services are running with detailed stats."""
    global _modbus, _parser, _logger
    
    from modbus_monitor.services.value_queue_service import value_queue_service
    
    status = {
        "running": _started,
        "architecture": "queue-based"
    }
    
    # Queue stats
    try:
        status["queue_stats"] = value_queue_service.get_queue_stats()
    except Exception as e:
        status["queue_stats"] = {"error": str(e)}
    
    # Parser stats
    if _parser:
        try:
            status["parser_stats"] = _parser.get_stats()
        except Exception as e:
            status["parser_stats"] = {"error": str(e)}
    
    # DataLogger stats
    if _logger:
        try:
            status["datalogger_stats"] = _logger.get_stats()
        except Exception as e:
            status["datalogger_stats"] = {"error": str(e)}
    
    return status
