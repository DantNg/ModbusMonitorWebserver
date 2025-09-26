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
from modbus_monitor.database import db as dbsync
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
                    
                    # SAVE ALL RTU TAG VALUES TO DATABASE - not just datalogger tags  
                    try:
                        ts = datetime.fromtimestamp(raw_value.timestamp)
                        dbsync.update_tag_latest_value(raw_value.tag_id, float(parsed_value), ts)
                    except Exception as db_err:
                        logger.warning(f"Failed to save latest value for RTU tag {raw_value.tag_id}: {db_err}")
                
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
                # Special case: hex/binary với 1 register
                if len(raw_val) == 1 and datatype in ("hex", "hexadecimal", "binary", "bin"):
                    val = self._parse_single_register(raw_val[0], datatype)
                else:
                    val = self._parse_multi_register(raw_val, datatype, raw_value)
            
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
        """Parse single 16-bit register value with enhanced datatype support"""
        
        if datatype in ("signed", "short", "int16"):
            # 16-bit signed
            if raw_val > 32767:
                return float(raw_val - 65536)
            else:
                return float(raw_val)
        
        elif datatype in ("unsigned", "word", "uint16", "ushort"):
            # 16-bit unsigned
            return float(raw_val)
        
        elif datatype in ("hex", "hexadecimal"):
            # Hex representation - return as string converted to float
            # Store hex string in format that can be converted back
            # For UI display, we'll need special handling
            return float(raw_val)  # Keep numeric value, UI can format as hex
        
        elif datatype in ("binary", "bin"):
            # Binary representation - return as numeric value
            # UI can format as binary display
            return float(raw_val)
        
        elif datatype in ("bit", "bool", "boolean"):
            # Boolean
            return float(1 if raw_val else 0)
        
        elif datatype in ("raw"):
            # Raw value
            return float(raw_val)
        
        else:
            # Default to unsigned
            return float(raw_val)
    
    def _parse_multi_register(self, raw_val: List[int], datatype: str, raw_value: RawModbusValue) -> Optional[float]:
        """Parse multi-register values (32-bit, 64-bit) with enhanced datatype support"""
        
        if len(raw_val) < 2:
            return None
        
        try:
            # Get word and byte order from device config
            word_order = getattr(raw_value, 'word_order', 'BA')
            byte_order = getattr(raw_value, 'byte_order', 'BigEndian')
            
            # FLOAT TYPES
            if datatype in ("float", "float32", "real"):
                # Float: Both BigEndian and LittleEndian devices should use AB word order
                # The difference is in how bytes within each register are interpreted
                float_word_order = 'AB'  # Always use AB for standard float
                
                # Debug logging
                print(f"DEBUG Float: device_byte_order={byte_order}, using_word_order={float_word_order}, raw_values={raw_val[0:2]}")
                
                result = self._parse_float32(raw_val[0], raw_val[1], 
                                         word_order=float_word_order, 
                                         byte_order=byte_order)
                print(f"DEBUG Float result: {result}")
                return result
            
            elif datatype in ("invert_float", "float_inverse", "floatinverse", "float-inverse"):
                # Invert Float: Use BA word order (opposite of standard)
                # BigEndian device -> use BA word order (inverted)
                # LittleEndian device -> use BA word order (inverted)
                invert_word_order = 'BA'  # Always use BA for invert_float
                
                # Debug logging
                print(f"DEBUG Invert Float: device_byte_order={byte_order}, using_word_order={invert_word_order}, raw_values={raw_val[0:2]}")
                
                result = self._parse_float32(raw_val[0], raw_val[1], 
                                         word_order=invert_word_order, 
                                         byte_order=byte_order)
                print(f"DEBUG Invert Float result: {result}")
                return result
            
            # DOUBLE TYPES
            elif datatype in ("double", "double64", "float64") and len(raw_val) >= 4:
                # Double: Map device byte_order to appropriate word_order for 64-bit values
                # BigEndian device -> use DCBA word order (big word first)
                # LittleEndian device -> use ABCD word order (little word first)
                double_word_order = 'DCBA' if byte_order == 'BigEndian' else 'ABCD'
                return self._parse_float64(raw_val[:4], word_order=double_word_order, byte_order=byte_order)
            
            elif datatype in ("invert_double", "double_inverse", "doubleinverse") and len(raw_val) >= 4:
                # Invert Double: Use opposite of device's natural word order
                # BigEndian device -> use ABCD word order (inverted)
                # LittleEndian device -> use DCBA word order (inverted)
                double_word_order = 'ABCD' if byte_order == 'BigEndian' else 'DCBA'
                return self._parse_float64(raw_val[:4], word_order=double_word_order, byte_order=byte_order)
            
            # LONG/INTEGER TYPES  
            elif datatype in ("long", "int32", "dint", "signed_long"):
                # Long: Map device byte_order to appropriate word_order
                long_word_order = 'BA' if byte_order == 'BigEndian' else 'AB'
                return float(self._parse_int32(raw_val[0], raw_val[1], word_order=long_word_order, signed=True))
            
            elif datatype in ("invert_long", "long_inverse", "longinverse"):
                # Invert Long: Use opposite of device's natural word order
                invert_word_order = 'AB' if byte_order == 'BigEndian' else 'BA'
                return float(self._parse_int32(raw_val[0], raw_val[1], word_order=invert_word_order, signed=True))
            
            elif datatype in ("ulong", "uint32", "dword", "udint", "unsigned_long"):
                # Unsigned Long: Map device byte_order to appropriate word_order
                ulong_word_order = 'BA' if byte_order == 'BigEndian' else 'AB'
                return float(self._parse_int32(raw_val[0], raw_val[1], word_order=ulong_word_order, signed=False))
            
            elif datatype in ("invert_ulong", "ulong_inverse", "ulonginverse"):
                # Invert Unsigned Long: Use opposite of device's natural word order
                invert_word_order = 'AB' if byte_order == 'BigEndian' else 'BA'
                return float(self._parse_int32(raw_val[0], raw_val[1], word_order=invert_word_order, signed=False))
            
            # LEGACY SUPPORT
            elif datatype in ("dword", "uint32", "udint"):
                # Legacy 32-bit unsigned integer
                return float(self._parse_int32(raw_val[0], raw_val[1], word_order=word_order, signed=False))
            
            elif datatype in ("dint", "int32", "int"):
                # Legacy 32-bit signed integer
                return float(self._parse_int32(raw_val[0], raw_val[1], word_order=word_order, signed=True))
            
            # HEX/BINARY - treat as single register
            elif datatype in ("hex", "hexadecimal"):
                # Hex representation - use first register
                return float(raw_val[0])
            
            elif datatype in ("binary", "bin"):
                # Binary representation - use first register  
                return float(raw_val[0])
            
            else:
                # Default: treat as 32-bit unsigned
                return float(self._parse_int32(raw_val[0], raw_val[1], word_order=word_order, signed=False))
                
        except Exception as e:
            logger.error(f"Error parsing multi-register value {raw_val} as {datatype}: {e}")
            return None
    
    def _parse_float32(self, reg1: int, reg2: int, word_order: str = "AB", byte_order: str = "BigEndian") -> float:
        """Parse 32-bit IEEE754 float from 2 registers"""
        
        if word_order == "AB":
            # Standard order: reg1 is high word, reg2 is low word
            w1, w2 = reg1, reg2
        else:
            # Inverse order: reg2 is high word, reg1 is low word  
            w1, w2 = reg2, reg1
        
        # Debug logging
        print(f"DEBUG _parse_float32: word_order={word_order}, byte_order={byte_order}, w1=0x{w1:04X}, w2=0x{w2:04X}")
        
        if byte_order == "BigEndian":
            # Big endian: pack each word as big endian, then combine
            b1 = w1.to_bytes(2, "big")
            b2 = w2.to_bytes(2, "big") 
            b = b1 + b2
            # Unpack as big-endian float
            result = struct.unpack(">f", b)[0]
        else:
            # Little endian: pack each word as little endian, then combine
            b1 = w1.to_bytes(2, "little")
            b2 = w2.to_bytes(2, "little")
            b = b1 + b2
            # Unpack as little-endian float
            result = struct.unpack("<f", b)[0]
        
        print(f"DEBUG _parse_float32: bytes={[hex(x) for x in b]}, result={result}")
        return result
    
    def _parse_uint32(self, reg1: int, reg2: int) -> int:
        """Parse 32-bit unsigned integer from 2 registers (AB word order)"""
        return (reg1 << 16) | reg2
    
    def _parse_float64(self, regs: List[int], word_order: str = 'ABCD', byte_order: str = 'BigEndian') -> float:
        """Parse 64-bit double from 4 registers with enhanced word/byte order support"""
        if len(regs) < 4:
            return math.nan
        
        try:
            # Apply word order
            if word_order == 'DCBA':
                # Reverse word order for invert_double
                words = [regs[3], regs[2], regs[1], regs[0]]
            else:
                # Normal ABCD order
                words = regs[:4]
            
            # Pack words to bytes
            bytes_data = b''
            for word in words:
                word_bytes = word.to_bytes(2, "big")
                if byte_order == "LittleEndian":
                    # Swap bytes within word
                    word_bytes = word_bytes[1:2] + word_bytes[0:1]
                bytes_data += word_bytes
            
            # Unpack as IEEE754 double
            result = struct.unpack(">d", bytes_data)[0]
            
            return result
            
        except Exception as e:
            logger.error(f"Error parsing float64: {e}")
            return math.nan
    
    def _parse_int32(self, reg1: int, reg2: int, word_order: str = 'AB', signed: bool = True) -> int:
        """Parse 32-bit integer with word order support"""
        
        try:
            # Apply word order
            if word_order == 'BA':
                high_word, low_word = reg2, reg1
            else:  # AB
                high_word, low_word = reg1, reg2
            
            # Combine words
            value = (high_word << 16) | low_word
            
            # Handle signed/unsigned
            if signed and value & 0x80000000:  # Check MSB for negative
                result = value - 0x100000000
            else:
                result = value
                
            return result
            
        except Exception as e:
            logger.error(f"Error parsing int32: {e}")
            return 0
    
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
