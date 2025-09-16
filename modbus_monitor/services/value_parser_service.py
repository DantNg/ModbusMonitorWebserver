"""
Value Parser Service
Consumer đọc raw values từ queue, parse thành final values và emit lên UI
"""
import threading
import time
import math
import struct
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

from modbus_monitor.services.value_queue_service import (
    ValueQueueService, RawModbusValue, value_queue_service
)
from modbus_monitor.services.socket_emission_manager import (
    SocketEmissionManager, get_emission_manager
)
from modbus_monitor.services.common import LatestCache

logger = logging.getLogger(__name__)

class ValueParserService:
    """
    Service parse raw modbus values và emit lên UI
    Consumer từ value queue
    """
    
    def __init__(self, cache: LatestCache):
        self.cache = cache
        self.running = False
        self._parser_thread = None
        
        # Get emission manager
        try:
            self.emission_manager = get_emission_manager()
        except Exception as e:
            logger.warning(f"Could not initialize emission manager: {e}")
            self.emission_manager = None
        
        # Stats
        self.stats = {
            'values_parsed': 0,
            'values_emitted': 0,
            'parse_errors': 0,
            'emission_errors': 0,
            'start_time': time.time()
        }
        self.stats_lock = threading.Lock()
        
        logger.info("ValueParserService initialized")
    
    def start(self):
        """Khởi động parser service"""
        if self._parser_thread is None or not self._parser_thread.is_alive():
            self.running = True
            self._parser_thread = threading.Thread(
                target=self._parser_loop,
                daemon=True,
                name="ValueParser"
            )
            self._parser_thread.start()
            logger.info("Value parser service started")
    
    def stop(self):
        """Dừng parser service"""
        self.running = False
        if self._parser_thread and self._parser_thread.is_alive():
            self._parser_thread.join(timeout=2.0)
        logger.info("Value parser service stopped")
    
    def _parser_loop(self):
        """Main parser loop - đọc từ queue và process"""
        logger.info("Value parser loop started")
        
        while self.running:
            try:
                # Lấy batch raw values từ queue
                raw_values = value_queue_service.get_parser_values_batch(max_count=50, timeout=0.5)
                
                if not raw_values:
                    continue
                
                # Parse batch
                parsed_results = self._parse_values_batch(raw_values)
                
                # Emit results grouped by device
                self._emit_parsed_results(parsed_results)
                
                # Update stats
                value_queue_service.mark_parsed(len(raw_values))
                
                with self.stats_lock:
                    self.stats['values_parsed'] += len(raw_values)
                
            except Exception as e:
                logger.error(f"Error in parser loop: {e}")
                time.sleep(0.1)  # Brief pause on error
        
        logger.info("Value parser loop stopped")
    
    def _parse_values_batch(self, raw_values: List[RawModbusValue]) -> Dict[int, Dict[str, Any]]:
        """
        Parse batch of raw values
        Returns: {device_id: {parsed_data}}
        """
        device_results = {}
        
        for raw_value in raw_values:
            try:
                # Parse individual value
                parsed_value = self._parse_single_value(raw_value)
                
                if parsed_value is not None:
                    device_id = raw_value.device_id
                    
                    # Initialize device result if needed
                    if device_id not in device_results:
                        device_results[device_id] = {
                            'device_id': device_id,
                            'tags': [],
                            'timestamp': raw_value.timestamp,
                            'seq': int(time.time() * 1000) % 10000  # Simple sequence
                        }
                    
                    # Add parsed tag to device result
                    device_results[device_id]['tags'].append({
                        'id': raw_value.tag_id,
                        'name': raw_value.tag_name,
                        'value': float(parsed_value),
                        'datatype': raw_value.data_type,
                        'unit': raw_value.unit,
                        'ts': datetime.fromtimestamp(raw_value.timestamp).strftime("%H:%M:%S")
                    })
                    
                    # Update cache
                    self.cache.set(raw_value.tag_id, raw_value.timestamp, parsed_value)
                
            except Exception as e:
                logger.error(f"Error parsing value for tag {raw_value.tag_name}: {e}")
                with self.stats_lock:
                    self.stats['parse_errors'] += 1
        
        return device_results
    
    def _parse_single_value(self, raw_value: RawModbusValue) -> Optional[float]:
        """
        Parse một raw value thành final value
        Sử dụng logic parsing từ modbus_service._extract()
        """
        raw_val = raw_value.raw_value
        datatype = (raw_value.data_type or "").strip().lower()
        scale = raw_value.scale or 1.0
        offset = raw_value.offset or 0.0
        
        if raw_val is None:
            return None
        
        try:
            # Handle single register values
            if isinstance(raw_val, int):
                val = self._parse_single_register(raw_val, datatype)
            
            # Handle multi-register values (list)
            elif isinstance(raw_val, list):
                val = self._parse_multi_register(raw_val, datatype)
            
            else:
                logger.warning(f"Unknown raw value type: {type(raw_val)}")
                return None
            
            # Apply scale and offset
            if val is not None and not math.isnan(val):
                final_val = val * scale + offset
                return final_val
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing value {raw_val} with datatype {datatype}: {e}")
            return None
    
    def _parse_single_register(self, raw_val: int, datatype: str) -> Optional[float]:
        """Parse single 16-bit register value"""
        
        if datatype in ("signed", "short", "int16"):
            # 16-bit signed
            if raw_val > 32767:
                return float(raw_val - 65536)
            else:
                return float(raw_val)
        
        elif datatype in ("unsigned", "word", "uint16", "ushort", "hex", "raw"):
            # 16-bit unsigned
            return float(raw_val)
        
        elif datatype in ("bit", "bool", "boolean"):
            # Boolean
            return float(1 if raw_val else 0)
        
        else:
            # Default to unsigned
            return float(raw_val)
    
    def _parse_multi_register(self, raw_val: List[int], datatype: str) -> Optional[float]:
        """Parse multi-register values (32-bit, 64-bit)"""
        
        if len(raw_val) < 2:
            return None
        
        try:
            if datatype in ("float", "float32", "real"):
                # 32-bit IEEE754 float - standard word order
                return self._parse_float32(raw_val[0], raw_val[1], word_order="AB")
            
            elif datatype in ("float_inverse", "floatinverse", "float-inverse"):
                # 32-bit IEEE754 float - inverse word order
                return self._parse_float32(raw_val[0], raw_val[1], word_order="BA")
            
            elif datatype in ("dword", "uint32", "udint"):
                # 32-bit unsigned integer
                return float(self._parse_uint32(raw_val[0], raw_val[1]))
            
            elif datatype in ("dint", "int32", "int"):
                # 32-bit signed integer
                uint_val = self._parse_uint32(raw_val[0], raw_val[1])
                if uint_val > 2147483647:
                    return float(uint_val - 4294967296)
                else:
                    return float(uint_val)
            
            elif datatype in ("long_inverse", "longinverse"):
                # 32-bit with inverse word order
                return float(self._parse_uint32(raw_val[1], raw_val[0]))
            
            elif datatype in ("double", "double_inverse") and len(raw_val) >= 4:
                # 64-bit double (requires 4 registers)
                return self._parse_float64(raw_val, datatype)
            
            else:
                # Default: treat as 32-bit unsigned
                return float(self._parse_uint32(raw_val[0], raw_val[1]))
                
        except Exception as e:
            logger.error(f"Error parsing multi-register value {raw_val} as {datatype}: {e}")
            return None
    
    def _parse_float32(self, reg1: int, reg2: int, word_order: str = "AB") -> float:
        """Parse 32-bit IEEE754 float from 2 registers"""
        if word_order == "AB":
            # Standard order: reg1 is high word, reg2 is low word
            w1, w2 = reg1, reg2
        else:
            # Inverse order: reg2 is high word, reg1 is low word  
            w1, w2 = reg2, reg1
        
        # Pack to bytes (big endian)
        b1 = w1.to_bytes(2, "big")
        b2 = w2.to_bytes(2, "big")
        b = b1 + b2
        
        # Unpack as float
        return struct.unpack(">f", b)[0]
    
    def _parse_uint32(self, reg1: int, reg2: int) -> int:
        """Parse 32-bit unsigned integer from 2 registers (AB word order)"""
        return (reg1 << 16) | reg2
    
    def _parse_float64(self, regs: List[int], datatype: str) -> float:
        """Parse 64-bit double from 4 registers"""
        if len(regs) < 4:
            return math.nan
        
        try:
            if "inverse" in datatype:
                # Inverse word order
                words = [regs[3], regs[2], regs[1], regs[0]]
            else:
                # Normal word order
                words = regs[:4]
            
            # Pack to bytes
            b = b"".join(w.to_bytes(2, "big") for w in words)
            
            # Unpack as double
            return struct.unpack(">d", b)[0]
            
        except Exception as e:
            logger.error(f"Error parsing float64: {e}")
            return math.nan
    
    def _emit_parsed_results(self, device_results: Dict[int, Dict[str, Any]]):
        """Emit parsed results grouped by device"""
        
        for device_id, result in device_results.items():
            try:
                if self.emission_manager:
                    # Use emission manager
                    self.emission_manager.emit_device_update(
                        device_id=f"dev{device_id}",
                        device_name=f"Device_{device_id}",  # Could get from config
                        unit=1,  # Could get from config
                        ok=True,
                        tags=result['tags'],
                        seq=result['seq']
                    )
                else:
                    # Direct emission fallback
                    from modbus_monitor.extensions import socketio
                    socketio.emit("modbus_update", {
                        "device_id": f"dev{device_id}",
                        "device_name": f"Device_{device_id}",
                        "unit": 1,
                        "ok": True,
                        "tags": result['tags'],
                        "seq": result['seq'],
                        "ts": datetime.now().strftime("%H:%M:%S")
                    }, room=f"dashboard_device_{device_id}")
                
                with self.stats_lock:
                    self.stats['values_emitted'] += len(result['tags'])
                
            except Exception as e:
                logger.error(f"Error emitting results for device {device_id}: {e}")
                with self.stats_lock:
                    self.stats['emission_errors'] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Lấy thống kê parser service"""
        with self.stats_lock:
            runtime = time.time() - self.stats['start_time']
            
            return {
                'runtime_seconds': runtime,
                'values_parsed': self.stats['values_parsed'],
                'values_emitted': self.stats['values_emitted'],
                'parse_errors': self.stats['parse_errors'],
                'emission_errors': self.stats['emission_errors'],
                'parse_rate_per_sec': self.stats['values_parsed'] / runtime if runtime > 0 else 0,
                'emission_rate_per_sec': self.stats['values_emitted'] / runtime if runtime > 0 else 0
            }
