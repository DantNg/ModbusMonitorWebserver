from __future__ import annotations
import threading, time, math, os
from queue import Queue
from typing import List, Dict, Tuple
from datetime import datetime
from modbus_monitor.database import db as dbsync
from modbus_monitor.services.common import LatestCache, utc_now
from pymodbus.exceptions import ModbusIOException
import asyncio
import struct
# pymodbus sync
from pymodbus.client import ModbusTcpClient, ModbusSerialClient
from pymodbus.exceptions import ModbusIOException
from pymodbus.exceptions import ConnectionException
import socketio

def _apply_sf(raw: float, scale: float, offset: float) -> float:
    return raw * (scale or 1.0) + (offset or 0.0)

def _unpack_u32(lo: int, hi: int, word_order: str) -> int:
    return ((hi << 16) | lo) if (word_order or "AB") == "AB" else ((lo << 16) | hi)

def _unpack_float(lo: int, hi: int, byte_order: str, word_order: str) -> float:
    w1, w2 = (hi, lo) if (word_order or "AB") == "AB" else (lo, hi)
    b1 = w1.to_bytes(2, "big")
    b2 = w2.to_bytes(2, "big")
    b = b1 + b2
    if (byte_order or "BigEndian") == "LittleEndian":
        b = b[1:2] + b[0:1] + b[3:4] + b[2:3]
    return struct.unpack(">f", b)[0]

class _DeviceReader:
    def __init__(self, dev_row: Dict, db_queue: Queue, push_queue: Queue, cache: LatestCache):
        self.d = dev_row
        self.dbq = db_queue
        self.pushq = push_queue
        self.cache = cache
        self.client = None
        self.byte_order = self.d.get("byte_order") or "BigEndian"
        self.word_order = self.d.get("word_order") or "AB"
        self.unit_id = int(self.d.get("unit_id") or 1)
        
        
        self.timeout = min((int(self.d.get("timeout_ms") or 200))/1000.0, 0.2)  # Fast 200ms timeout
        self._connected = False
        self._backoff = 1.0
        self._next_retry_ts = 0.0
        self._seq = 0  # Sequence counter for debugging
        self._device_id_str = f"dev{self.d['id']}"  # String ID for socket emissions

    def _connect(self) -> bool:
        # if os.getenv("FAKE_MODBUS") == "1":
        #     self.client = "FAKE"
        #     return True

        try:
            if self.d["protocol"] == "ModbusTCP":
                host = self.d.get("host")
                port = int(self.d.get("port") or 502)
                print(f"🔌 Connecting to ModbusTCP: host={host}, port={port}")
                self.client = ModbusTcpClient(host, port=port, timeout=self.timeout)
            else:
                serial_port = self.d.get("serial_port")
                baudrate = int(self.d.get("baudrate") or 9600)
                parity = self.d.get("parity") or "N"
                print(f"🔌 Connecting to ModbusRTU: port={serial_port}, baudrate={baudrate}, parity={parity}")
                self.client = ModbusSerialClient(
                    port=serial_port,
                    baudrate=baudrate,
                    bytesize=int(self.d.get("bytesize") or 8),
                    parity=parity,
                    stopbits=int(self.d.get("stopbits") or 1),
                    timeout=self.timeout
                )
            
            connected = self.client.connect()
            status = "SUCCESS" if connected else "FAILED" 
            
            if not connected and self.d["protocol"] == "ModbusTCP":
                print(f"💡 TCP connection tips: Check if device is online at {self.d.get('host')}:{self.d.get('port', 502)}")
            elif not connected and self.d["protocol"] == "ModbusRTU":
                print(f"💡 RTU connection tips: Check COM port, baudrate, and cable connection")
                
            return connected
            
        except Exception as e:
            print(f"❌ Connection error for {self.d.get('name')}: {e}")
            return False

    def _ensure_connected(self) -> bool:
        now = time.time()
        if self._connected:
            # Test connection periodically by attempting a simple operation
            if hasattr(self, '_last_connection_test') and now - self._last_connection_test > 30:
                if not self._test_connection():
                    print(f"🔄 Device {self.d.get('name')} ({self.d['protocol']}): Connection lost, reconnecting...")
                    self._connected = False
                    self._close()
                else:
                    self._last_connection_test = now
            
            if self._connected:
                return True
        
        if now < self._next_retry_ts:
            return False
            
        print(f"🔄 Device {self.d.get('name')} ({self.d['protocol']}): Attempting connection (retry #{getattr(self, '_retry_count', 0) + 1})")
        ok = self._connect()
        
        if ok:
            self._connected = True
            self._backoff = 1.0
            self._retry_count = 0
            self._last_connection_test = now
            # print(f"✅ Device {self.d.get('name')} ({self.d['protocol']}): Connection successful")
            
            # Emit connection success to UI
            try:
                from modbus_monitor.extensions import socketio
                socketio.emit("modbus_update", {
                    "device_id": self._device_id_str,
                    "device_name": self.d.get("name", "Unknown"),
                    "unit": self.d.get("unit_id", 1),
                    "ok": True,
                    "status": "connected",
                    "seq": self._seq,
                    "ts": time.time()
                }, room=f"dashboard_device_{self.d['id']}")
            except Exception:
                pass
        else:
            self._retry_count = getattr(self, '_retry_count', 0) + 1
            # Faster retry for first few attempts, then backoff
            if self._retry_count <= 3:
                retry_delay = 2.0  # Quick retry for first 3 attempts
            elif self._retry_count <= 10:
                retry_delay = 5.0  # Medium delay for next 7 attempts
            else:
                retry_delay = min(self._backoff, 30.0)  # Longer backoff after 10 attempts
                self._backoff = min(self._backoff * 1.5, 30.0)
            
            self._next_retry_ts = now + retry_delay
            print(f"❌ Device {self.d.get('name')} ({self.d['protocol']}): Connection failed, retry in {retry_delay}s (attempt #{self._retry_count})")
        
        return ok

    def _test_connection(self) -> bool:
        """Test if the connection is still alive by performing a simple read operation"""
        try:
            if self.client is None:
                return False
                
            # Try to read a single register/coil to test connection
            if self.d["protocol"] == "ModbusTCP":
                # For TCP, try to read 1 holding register
                result = self.client.read_holding_registers(0, 1, slave=self.unit_id)
                return not result.isError()
            else:
                # For RTU, try to read 1 holding register  
                result = self.client.read_holding_registers(0, 1, slave=self.unit_id)
                return not result.isError()
        except Exception:
            return False

    def _close(self):
        try:
            if self.client and self.client != "FAKE":
                self.client.close()
        except Exception:
            pass
        self.client = None
        self._connected = False
    def _normalize_hr_address(self, addr: int) -> int:
        """
        Chuẩn hoá địa chỉ Holding Register về 0-based.
        - Nếu DB lưu kiểu 40001/30001/10001 thì trừ đi base tương ứng.
        - Nếu đã là 0-based (0,1,2,...) thì giữ nguyên.
        """
        a = int(addr)
        if a >= 40001:      # Holding Registers (4xxxx)
            return a - 40001
        if a >= 30001:      # Input Registers   (3xxxx) -> nếu bạn đọc IR thì đổi hàm đọc
            return a - 30001
        if a >= 10001:      # Coils/Discrete (1/2xxxx) -> nếu bạn đọc coil/discrete thì đổi hàm đọc
            return a - 10001
        return a            # giả định đã 0-based

    def _read_registers(self, address: int, count: int, function_code: int = None):
        """
        Read data from Modbus device using specified function code.
        function_code: 1=Read Coils, 2=Read Discrete Inputs, 3=Read Holding Registers, 4=Read Input Registers
        """
        if count <= 0:
            raise ModbusIOException("count must be > 0")

        # Use device default function code if not specified
        if function_code is None:
            function_code = self.d.get("default_function_code", 3)

        start_read_byte = self._normalize_hr_address(address)
        
        try:
            if self.d["protocol"] in ("ModbusTCP", "ModbusRTU"):
                if self.client is None:
                    if not self._ensure_connected():
                        raise ConnectionException("Failed to connect to Modbus client")


                # Choose appropriate read function based on function code
                if function_code == 1:  # Read Coils
                    rr = self.client.read_coils(start_read_byte, count=count, slave=int(self.d["unit_id"]))
                    if rr.isError():
                        print(f"❌ FC01 Read Coils error: {rr}")
                        return None
                    # print(f"✅ FC01 Read Coils success: {[int(bit) for bit in rr.bits[:count]]}")
                    return [int(bit) for bit in rr.bits[:count]]
                elif function_code == 2:  # Read Discrete Inputs
                    rr = self.client.read_discrete_inputs(start_read_byte, count=count, slave=int(self.d["unit_id"]))
                    if rr.isError():
                        print(f"❌ FC02 Read Discrete Inputs error: {rr}")
                        return None
                    # print(f"✅ FC02 Read Discrete Inputs success: {[int(bit) for bit in rr.bits[:count]]}")
                    return [int(bit) for bit in rr.bits[:count]]
                elif function_code == 3:  # Read Holding Registers
                    rr = self.client.read_holding_registers(start_read_byte, count=count, slave=int(self.d["unit_id"]))
                    if rr.isError():
                        print(f"❌ FC03 Read Holding Registers error: {rr}")
                        return None
                    # print(f"✅ FC03 Read Holding Registers success: {rr.registers}")
                    return rr.registers
                elif function_code == 4:  # Read Input Registers
                    rr = self.client.read_input_registers(start_read_byte, count=count, slave=int(self.d["unit_id"]))
                    if rr.isError():
                        print(f"❌ FC04 Read Input Registers error: {rr}")
                        return None
                    # print(f"✅ FC04 Read Input Registers success: {rr.registers}")
                    return rr.registers
                else:
                    raise ValueError(f"Unsupported function code: {function_code}")
                    
            else:
                raise ValueError("Unsupported protocol: {}".format(self.d["protocol"]))

        except (ConnectionException, ModbusIOException, IOError) as e:
            self._connected = False  # Mark as disconnected to trigger reconnection
            self._close()
            # Trigger immediate retry by resetting backoff for connection errors
            if isinstance(e, ConnectionException):
                self._next_retry_ts = time.time() + 1.0  # Quick retry for connection errors
            # Return None values to indicate read failure
            return [None] * count  

        except Exception as e:
            print("Unexpected error during Modbus read:", e)
            self._close()
            return [None] * count

    def _extract(self, regs: list[int], offset: int, datatype: str, scale: float, offs: float) -> float | int | None:
        """
        Chuyển regs -> giá trị thật theo datatype.
        Hỗ trợ các datatype: Signed, Unsigned, Hex, Raw, Float, Binary, Double, Float_inverse, Long, Double_inverse
        và các alias: word/uint16/ushort, short/int16, dword/uint32/udint, dint/int32/int,
                    float/real, bit/bool/boolean.
        Tôn trọng self.word_order ('AB'|'BA') và self.byte_order ('BigEndian'|'LittleEndian').
        Trả về int nếu không có scale/offset và là số nguyên, float nếu có thập phân.
        """
        name = (datatype or "").strip().lower()

        # Kiểm tra bounds và None values
        if offset >= len(regs) or regs[offset] is None:
            return math.nan

        # --- helpers ---
        def _two_words():
            if offset + 1 >= len(regs) or regs[offset + 1] is None:
                return None, None, None
            lo, hi = regs[offset], regs[offset+1]
            # word order: AB = hi->lo, BA = lo->hi
            w1, w2 = (hi, lo) if (self.word_order or "AB") == "AB" else (lo, hi)
            b = w1.to_bytes(2, "big") + w2.to_bytes(2, "big")
            # byte order trong từng word
            if (self.byte_order or "BigEndian") == "LittleEndian":
                b = b[1:2] + b[0:1] + b[3:4] + b[2:3]
            return lo, hi, b

        def _four_words():
            """Helper for 64-bit datatypes (Double, Long)"""
            if offset + 3 >= len(regs) or any(regs[offset + i] is None for i in range(4)):
                return None
            # Get 4 words and pack according to word order
            words = [regs[offset + i] for i in range(4)]
            if (self.word_order or "AB") == "AB":
                # Normal order: w0,w1,w2,w3
                b = words[0].to_bytes(2, "big") + words[1].to_bytes(2, "big") + words[2].to_bytes(2, "big") + words[3].to_bytes(2, "big")
            else:
                # Inverse order: w3,w2,w1,w0
                b = words[3].to_bytes(2, "big") + words[2].to_bytes(2, "big") + words[1].to_bytes(2, "big") + words[0].to_bytes(2, "big")
            
            # Apply byte order
            if (self.byte_order or "BigEndian") == "LittleEndian":
                # Swap bytes within each word
                result = b""
                for i in range(0, 8, 2):
                    result += b[i+1:i+2] + b[i:i+1]
                b = result
            return b

        try:
            # === New datatypes from dropdown ===
            
            # Signed (16-bit signed)
            if name in ("signed", "short", "int16"):
                raw_val = regs[offset]
                if raw_val > 32767:
                    val = raw_val - 65536
                else:
                    val = raw_val

            # Unsigned (16-bit unsigned)  
            elif name in ("unsigned", "word", "uint16", "ushort"):
                val = regs[offset]

            # Hex (display as hex but store as int)
            elif name in ("hex", "raw"):
                val = regs[offset]  # Same as unsigned but UI might display differently

            # Float (32-bit IEEE754)
            elif name in ("float", "float32", "real"):
                print("Decoding float at offset", offset, "with regs:", regs)
                lo, hi, b = _two_words()
                if b is None:
                    return math.nan
                val = float(struct.unpack(">f", b)[0])

            # Float_inverse (32-bit IEEE754 with inverse word order)
            elif name in ("float_inverse", "floatinverse", "float-inverse"):
                if offset + 1 >= len(regs) or regs[offset + 1] is None:
                    return math.nan
                lo, hi = regs[offset], regs[offset+1]
                # Force inverse word order for this datatype
                w1, w2 = (lo, hi)  # Opposite of normal AB order
                b = w1.to_bytes(2, "big") + w2.to_bytes(2, "big")
                if (self.byte_order or "BigEndian") == "LittleEndian":
                    b = b[1:2] + b[0:1] + b[3:4] + b[2:3]
                val = float(struct.unpack(">f", b)[0])

            # Binary (boolean/bit)
            elif name in ("binary", "bit", "bool", "boolean"):
                val = 1 if regs[offset] != 0 else 0

            # Double (64-bit IEEE754)
            elif name in ("double", "float64"):
                b = _four_words()
                if b is None:
                    return math.nan
                val = float(struct.unpack(">d", b)[0])

            # Double_inverse (64-bit IEEE754 with inverse word order)
            elif name in ("double_inverse", "doubleinverse", "double-inverse"):
                if offset + 3 >= len(regs) or any(regs[offset + i] is None for i in range(4)):
                    return math.nan
                # Force inverse word order
                words = [regs[offset + i] for i in range(4)]
                b = words[3].to_bytes(2, "big") + words[2].to_bytes(2, "big") + words[1].to_bytes(2, "big") + words[0].to_bytes(2, "big")
                if (self.byte_order or "BigEndian") == "LittleEndian":
                    result = b""
                    for i in range(0, 8, 2):
                        result += b[i+1:i+2] + b[i:i+1]
                    b = result
                val = float(struct.unpack(">d", b)[0])

            # Long (64-bit signed integer)
            elif name in ("long", "int64"):
                b = _four_words()
                if b is None:
                    return math.nan
                val = struct.unpack(">q", b)[0]  # signed 64-bit

            # === Legacy aliases for backward compatibility ===
            
            # 32-bit unsigned
            elif name in ("dword", "uint32", "udint"):
                lo, hi, b = _two_words()
                if lo is None or hi is None:
                    return math.nan
                u32 = (hi << 16) | lo if (self.word_order or "AB") == "AB" else (lo << 16) | hi
                val = u32

            # 32-bit signed
            elif name.lower() in ("dint", "int32", "int"):
                lo, hi, b = _two_words()
                if lo is None or hi is None:
                    return math.nan
                u32 = (hi << 16) | lo if (self.word_order or "AB") == "AB" else (lo << 16) | hi
                if u32 >= 2147483648:  # 2^31
                    val = u32 - 4294967296  # 2^32
                else:
                    val = u32

            else:
                # Datatype chưa biết → trả NaN để UI thấy rõ
                print(f"⚠️ Unknown datatype: {datatype}")
                return math.nan

            # Áp dụng scale/offset (chỉ convert thành float khi cần thiết)
            scale_factor = scale if scale is not None else 1.0
            offset_value = offs if offs is not None else 0.0
            
            # Chỉ thực hiện phép tính khi có scale/offset khác mặc định
            if scale_factor != 1.0 or offset_value != 0.0:
                val = val * scale_factor + offset_value
            # Else: giữ nguyên val (có thể là int)

            # Nếu là float/double/real thì luôn trả về float
            if name in ("float", "float32", "real", "float_inverse", "floatinverse", "float-inverse", 
                       "double", "float64", "double_inverse", "doubleinverse", "double-inverse"):
                rounded_val = round(val, 6)  # More precision for double
                if rounded_val == 0.0:
                    rounded_val = 0.0
                return float(rounded_val)

            # Nếu là kiểu số nguyên
            if name in ("signed", "unsigned", "word", "uint16", "ushort", "short", "int16", "hex", "raw",
                       "dword", "uint32", "udint", "dint", "int32", "int", "long", "int64"):
                # Nếu val vẫn là int và chưa bị modify bởi scale/offset
                if isinstance(val, int):
                    return val
                # Nếu đã thành float, kiểm tra xem có phải là số nguyên không
                elif isinstance(val, float):
                    if abs(val - round(val)) < 1e-9:  # So sánh với epsilon để tránh floating point error
                        return int(round(val))
                    else:
                        return round(val, 2)
                else:
                    return val

            # Bit/bool/boolean/binary: trả về int 0 hoặc 1
            if name in ("binary", "bit", "bool", "boolean"):
                if isinstance(val, int):
                    return val
                else:
                    return int(round(val))

            # Default fallback
            return val

            # Trường hợp còn lại
            return val

        except Exception:
            return math.nan

        except Exception:
            return math.nan
    def _calculate_read_range(self, tags: List[Dict]) -> Tuple[int, int]:
        """Tính toán địa chỉ bắt đầu và số lượng register cần đọc để cover tất cả tags."""
        if not tags:
            return 0, 0
        
        # Tính địa chỉ đã normalize và số register cần cho từng tag
        tag_ranges = []
        for t in tags:
            addr = self._normalize_hr_address(int(t["address"]))
            dt = t["datatype"]
            count = 1 if dt.lower() in ("word", "uint16", "ushort", "short", "int16", "bit", "bool", "boolean") else 2
            tag_ranges.append((addr, addr + count - 1))
        
        # Tìm range tối thiểu bao phủ tất cả
        min_addr = min(start for start, _ in tag_ranges)
        max_addr = max(end for _, end in tag_ranges)
        
        return min_addr, max_addr - min_addr + 1

    def _encode_value_for_write(self, value: float, datatype: str) -> List[int]:
        """
        Chuyển đổi giá trị thành danh sách registers để ghi.
        Hỗ trợ các datatype: Signed, Unsigned, Hex, Raw, Float, Binary, Double, Float_inverse, Long, Double_inverse
        Tôn trọng self.word_order ('AB'|'BA') và self.byte_order ('BigEndian'|'LittleEndian').
        """
        name = (datatype or "").strip().lower()
        
        try:
            # === New datatypes ===
            
            # Signed (16-bit signed)
            if name in ("signed", "short", "int16"):
                val = int(value)
                if val < 0:
                    val = (1 << 16) + val  # Two's complement
                return [val & 0xFFFF]
                
            # Unsigned (16-bit unsigned)
            elif name in ("unsigned", "word", "uint16", "ushort"):
                val = int(value) & 0xFFFF
                return [val]
                
            # Hex/Raw (same as unsigned)
            elif name in ("hex", "raw"):
                val = int(value) & 0xFFFF
                return [val]
                
            # Float (32-bit IEEE754)
            elif name in ("float", "float32", "real"):
                import struct
                packed = struct.pack('>f', float(value))
                w1, w2 = struct.unpack('>HH', packed)
                
                # Áp dụng byte order trong từng word
                if (self.byte_order or "BigEndian") == "LittleEndian":
                    w1 = ((w1 & 0xFF) << 8) | ((w1 >> 8) & 0xFF)
                    w2 = ((w2 & 0xFF) << 8) | ((w2 >> 8) & 0xFF)
                
                # word order: AB = w1->w2, BA = w2->w1
                return [w1, w2] if (self.word_order or "AB") == "AB" else [w2, w1]
                
            # Float_inverse (32-bit IEEE754 with forced inverse word order)
            elif name in ("float_inverse", "floatinverse", "float-inverse"):
                import struct
                packed = struct.pack('>f', float(value))
                w1, w2 = struct.unpack('>HH', packed)
                
                if (self.byte_order or "BigEndian") == "LittleEndian":
                    w1 = ((w1 & 0xFF) << 8) | ((w1 >> 8) & 0xFF)
                    w2 = ((w2 & 0xFF) << 8) | ((w2 >> 8) & 0xFF)
                
                # Force inverse order (opposite of AB)
                return [w2, w1]  # Always inverse regardless of word_order setting
                
            # Binary (boolean/bit)
            elif name in ("binary", "bit", "bool", "boolean"):
                return [1 if value else 0]
                
            # Double (64-bit IEEE754)
            elif name in ("double", "float64"):
                import struct
                packed = struct.pack('>d', float(value))
                words = list(struct.unpack('>HHHH', packed))
                
                if (self.byte_order or "BigEndian") == "LittleEndian":
                    words = [((w & 0xFF) << 8) | ((w >> 8) & 0xFF) for w in words]
                
                # Apply word order
                if (self.word_order or "AB") != "AB":
                    words = words[::-1]  # Reverse word order
                    
                return words
                
            # Double_inverse (64-bit IEEE754 with forced inverse word order)
            elif name in ("double_inverse", "doubleinverse", "double-inverse"):
                import struct
                packed = struct.pack('>d', float(value))
                words = list(struct.unpack('>HHHH', packed))
                
                if (self.byte_order or "BigEndian") == "LittleEndian":
                    words = [((w & 0xFF) << 8) | ((w >> 8) & 0xFF) for w in words]
                
                # Force inverse order
                return words[::-1]
                
            # Long (64-bit signed integer)
            elif name in ("long", "int64"):
                import struct
                val = int(value)
                packed = struct.pack('>q', val)
                words = list(struct.unpack('>HHHH', packed))
                
                if (self.byte_order or "BigEndian") == "LittleEndian":
                    words = [((w & 0xFF) << 8) | ((w >> 8) & 0xFF) for w in words]
                
                if (self.word_order or "AB") != "AB":
                    words = words[::-1]
                    
                return words
                
            # === Legacy datatypes for backward compatibility ===
                
            elif name in ("dword", "uint32", "udint"):
                # 32-bit unsigned
                val = int(value) & 0xFFFFFFFF
                lo = val & 0xFFFF
                hi = (val >> 16) & 0xFFFF
                return [hi, lo] if (self.word_order or "AB") == "AB" else [lo, hi]
                
            elif name.lower() in ("dint", "int32", "int"):
                # 32-bit signed
                val = int(value)
                if val < 0:
                    val = (1 << 32) + val  # Two's complement
                val = val & 0xFFFFFFFF
                lo = val & 0xFFFF
                hi = (val >> 16) & 0xFFFF
                return [hi, lo] if (self.word_order or "AB") == "AB" else [lo, hi]
                
            else:
                raise ValueError(f"Unsupported datatype for write: {datatype}")
                
        except Exception as e:
            raise ValueError(f"Error encoding value {value} for datatype {datatype}: {e}")

    def write_tag_value(self, tag_id: int, value: float) -> bool:
        """
        Ghi giá trị vào một tag cụ thể dựa trên function code của tag.
        Returns True if successful, False otherwise.
        """
        if not self._ensure_connected():
            return False
            
        try:
            # Lấy thông tin tag từ database
            tag = dbsync.get_tag(tag_id)
            if not tag or tag["device_id"] != self.d["id"]:
                print(f"Tag {tag_id} not found or doesn't belong to device {self.d['name']}")
                return False
            
            # Xác định function code - ưu tiên function code của tag, sau đó là device default
            function_code = tag.get("function_code") or self.d.get("default_function_code", 3)
            
            # Chỉ cho phép ghi vào function code có thể ghi được
            if function_code == 1:  # Coils - có thể ghi (function code 05/15)
                return self._write_coil(tag, value)
            elif function_code == 2:  # Discrete Inputs - READ ONLY
                print(f"Cannot write to discrete input tag {tag['name']} (function code 02)")
                return False
            elif function_code == 3:  # Holding Registers - có thể ghi (function code 06/16)
                return self._write_holding_register(tag, value)
            elif function_code == 4:  # Input Registers - READ ONLY  
                print(f"Cannot write to input register tag {tag['name']} (function code 04)")
                return False
            else:
                print(f"Unsupported function code {function_code} for tag {tag['name']}")
                return False
                
        except Exception as e:
            print(f"Error writing to tag {tag_id}: {e}")
            self._close()
            return False

    def _write_coil(self, tag: dict, value: float) -> bool:
        """Ghi giá trị vào coil (function code 05/15)"""
        try:
            # Convert value to boolean
            bool_value = bool(int(value))
            write_addr = self._normalize_hr_address(int(tag["address"]))
            
            # Use write_coil for single coil (function code 05)
            result = self.client.write_coil(write_addr, bool_value, slave=int(self.d["unit_id"]))
            
            if result.isError():
                print(f"Modbus coil write error for tag {tag['name']}: {result}")
                return False
                
            print(f"Successfully wrote coil value {bool_value} to tag {tag['name']} (device: {self.d['name']})")
            return True
            
        except Exception as e:
            print(f"Error writing coil to tag {tag['name']}: {e}")
            return False

    def _write_holding_register(self, tag: dict, value: float) -> bool:
        """Ghi giá trị vào holding register (function code 06/16)"""
        try:
            # Áp dụng scale/offset ngược (từ giá trị thật về raw)
            scale = float(tag.get("scale") or 1.0)
            offset = float(tag.get("offset") or 0.0)
            raw_value = (value - offset) / scale if scale != 0 else value
            
            # Encode giá trị thành registers
            registers = self._encode_value_for_write(raw_value, tag["datatype"])
            
            # Tính địa chỉ ghi
            write_addr = self._normalize_hr_address(int(tag["address"]))
            
            # Thực hiện ghi
            if len(registers) == 1:
                # Ghi single register (function code 06)
                result = self.client.write_register(write_addr, registers[0], slave=int(self.d["unit_id"]))
            else:
                # Ghi multiple registers (function code 16)
                result = self.client.write_registers(write_addr, registers, slave=int(self.d["unit_id"]))
            
            if result.isError():
                print(f"Modbus register write error for tag {tag['name']}: {result}")
                return False
                
            print(f"Successfully wrote register value {value} to tag {tag['name']} (device: {self.d['name']})")
            return True
            
        except Exception as e:
            print(f"Error writing register to tag {tag['name']}: {e}")
            return False

    def loop_once(self):
        """Đọc 1 vòng cho device này với timing tracking và immediate socket emission."""
        t0 = time.perf_counter()  # Start timing
        
        if not self._ensure_connected():
            # Emit disconnection status to UI
            from modbus_monitor.extensions import socketio
            try:
                socketio.emit("modbus_update", {
                    "device_id": self._device_id_str,
                    "device_name": self.d.get("name", "Unknown"),
                    "unit": self.d.get("unit_id", 1),
                    "ok": False,
                    "error": "Connection failed",
                    "status": "disconnected",
                    "seq": self._seq,
                    "ts": time.time()
                }, room=f"dashboard_device_{self.d['id']}")
            except Exception:
                pass
            return  # chưa đến giờ retry, quay lại sau

        tags = dbsync.list_tags(self.d["id"])
        if not tags:
            return
        # print(f"Reading device {self.d['name']} with {len(tags)} tags")
        # Group tags by function code
        function_code_groups = {}
        device_default_fc = self.d.get("default_function_code", 3)
        
        for tag in tags:
            fc = tag.get("function_code") or device_default_fc
            if fc not in function_code_groups:
                function_code_groups[fc] = []
            function_code_groups[fc].append(tag)
        ts = utc_now()
        self._seq += 1
        all_successful_tags = []  # Track all successfully read tags for immediate emission
        
        # Process each function code group separately
        for function_code, fc_tags in function_code_groups.items():
            try:
                # Calculate read range for this function code group
                start_addr, count = self._calculate_read_range(fc_tags)
                if count == 0:
                    continue

                # Read all registers/bits for this function code
                bulk_data = self._read_registers(start_addr, count, function_code)
                if not bulk_data or all(r is None for r in bulk_data):
                    continue
                # Parse each tag from bulk data
                for t in fc_tags:
                    try:
                        addr = self._normalize_hr_address(int(t["address"]))
                        dt = t["datatype"]
                        scale = float(t.get("scale") or 1.0)
                        offs = float(t.get("offset") or 0.0)
                        
                        # Calculate offset in bulk_data array
                        offset_in_bulk = addr - start_addr
                
                        # For bit-based function codes (1,2), handle differently
                        if function_code in [1, 2]:
                            # For coils/discrete inputs, each element is already a single bit
                            if 0 <= offset_in_bulk < len(bulk_data):
                                raw_val = bulk_data[offset_in_bulk]
                                val = float(raw_val) * scale + offs if raw_val is not None else None
                            else:
                                val = None
                        else:
                            # For register-based function codes (3,4), extract according to datatype
                            val = self._extract(bulk_data, offset_in_bulk, dt, scale, offs)
                        # Save to cache and queue
                        self.cache.set(int(t["id"]), ts, val)
                        # print(val)
                        if val is not None:
                            self.dbq.put((int(t["id"]), ts, float(val)))
                            # Track for immediate emission
                            all_successful_tags.append({
                                "id": int(t["id"]),
                                "name": t.get("name", "tag_test"),
                                "value": float(val),
                                "datatype":t["datatype"],
                                "ts": ts.strftime("%H:%M:%S") if ts else "--:--:--"
                            })

                    except Exception as e:
                        print(f"Error processing tag {t.get('name', '?')}: {e}")
                        continue

            except Exception as e:
                print(f"Error reading function code {function_code} for device {self.d['name']}: {e}")
                self._close()
                continue
        
        # Calculate latency and emit immediately to Socket.IO
        latency_ms = int((time.perf_counter() - t0) * 1000)
        
        if all_successful_tags:
            # Immediate socket emission for this device
            from modbus_monitor.extensions import socketio
            
            # Emit to relevant subdashboards
            try:
                subdashboards = dbsync.list_subdashboards() or []
                tag_ids = [tag["id"] for tag in all_successful_tags]
                
                for subdash in subdashboards:
                    subdash_id = subdash['id']
                    subdash_tag_ids = [t['id'] for t in dbsync.get_subdashboard_tags(subdash_id) or []]
                    # Filter tags that belong to this subdashboard
                    subdash_tags = [tag for tag in all_successful_tags if tag['id'] in subdash_tag_ids]
                    if subdash_tags:
                        socketio.emit("modbus_update", {
                            "device_id": self._device_id_str,
                            "device_name": self.d.get("name", "Unknown"),
                            "unit": self.d.get("unit_id", 1),
                            "ok": True,
                            "tags": subdash_tags,
                            "seq": self._seq,
                            "latency_ms": latency_ms,
                            "ts": time.time(),
                            "tag_count": len(subdash_tags)
                        }, room=f"subdashboard_{subdash_id}")
            except Exception as e:
                print(f"Error emitting to subdashboards: {e}")
                

    def loop_with_timing(self, start_epoch: float, barrier: threading.Barrier):
        """
        High-precision loop with barrier synchronization and anti-drift timing.
        Based on async_modbus.py approach.
        """
        try:
            # IMPORTANT: Always wait for barrier regardless of connection status
            # This prevents blocking other devices when one device fails to connect
            print(f"Device {self.d['name']}: Waiting for synchronized start...")
            try:
                barrier.wait(timeout=10.0)  # 10 second timeout to prevent infinite wait
            except threading.BrokenBarrierError:
                print(f"Device {self.d['name']}: Barrier broken, starting independently")
            except Exception as e:
                print(f"Device {self.d['name']}: Barrier error: {e}, starting independently")
            
            # Wait until the synchronized start time
            now = time.monotonic()
            if now < start_epoch:
                time.sleep(start_epoch - now)
            
            # print(f"Device {self.d['name']}: Starting monitoring loop...")
            
            # High-speed mode intervals
            interval = max(self._get_optimal_interval(), 0.05)  # Ultra-fast minimum 50ms (20 updates/sec)
           
            next_run = start_epoch
            
            while True:  # Run indefinitely until thread is stopped
                now = time.monotonic()
                if now < next_run:
                    time.sleep(next_run - now)
                
                # Execute one read cycle (this will handle connection internally)
                try:
                    self.loop_once()
                except Exception as e:
                    print(f"[Device {self.d['name']}] Error in loop_once: {e}")
                    # Emit error status
                    from modbus_monitor.extensions import socketio
                    socketio.emit("modbus_update", {
                        "device_id": self._device_id_str,
                        "device_name": self.d.get("name", "Unknown"),
                        "unit": self.d.get("unit_id", 1),
                        "ok": False,
                        "error": str(e),
                        "seq": self._seq,
                        "ts": time.time()
                    }, room=f"dashboard_device_{self.d['id']}")
                
                # Schedule next run (anti-drift)
                next_run += interval
                
                # Skip missed cycles to catch up (prevents drift)
                while time.monotonic() >= next_run:
                    next_run += interval
                    
        except Exception as e:
            print(f"Device {self.d['name']}: Fatal error in timing loop: {e}")
        finally:
            self._close()
    
    def _get_optimal_interval(self) -> float:
        """Get the optimal read interval for this device based on tag loggers."""
        try:
            tag_logger_map = dbsync.get_tag_logger_map(self.d["id"])
            intervals = [v["interval_sec"] for v in tag_logger_map.values()]
            
            # Ultra-high-speed mode: 50ms to 500ms range
            min_interval = min(intervals) if intervals else 0.2
            return max(min(min_interval, 0.5), 0.05)  # 50ms to 500ms
         
        except Exception:
            # Default based on mode
            return 0.2 # Default 200ms

class ModbusService:
    """High-performance multi-threaded Modbus service with precision timing and immediate socket emission."""
    def __init__(self, db_queue: Queue, push_queue: Queue, cache: LatestCache):
        self.dbq = db_queue
        self.pushq = push_queue
        self.cache = cache
        self._stop = threading.Event()
        self._threads: Dict[int, threading.Thread] = {}
        self._readers: Dict[int, _DeviceReader] = {}  # Store device readers for write access
        self._barrier = None  # Synchronization barrier for coordinated start

    def start(self):
        devices = dbsync.list_devices()
        if not devices:
            print("No devices found for Modbus monitoring")
            return
            
        print(f"Starting high-speed Modbus service for {len(devices)} devices")
        
        # Create barrier for synchronized start
        self._barrier = threading.Barrier(len(devices))
        
        # Calculate synchronized start epoch (next full second + 1 second buffer)
        start_epoch = math.ceil(time.monotonic()) + 1
        print(f"Synchronized start scheduled for epoch: {start_epoch}")
        
        # Start devices with individual error handling
        started_devices = 0
        for d in devices:
            try:
                reader = _DeviceReader(d, db_queue=self.dbq, push_queue=self.pushq, cache=self.cache)
                self._readers[d["id"]] = reader  # Store reader reference
                
                # Use high-precision timing loop
                t = threading.Thread(
                    target=reader.loop_with_timing, 
                    args=(start_epoch, self._barrier), 
                    daemon=True, 
                    name=f"Modbus-{d['name']}"
                )
                t.start()
                self._threads[d["id"]] = t
                started_devices += 1
                # print(f"✅ Started thread for device: {d['name']} ({d['protocol']})")
                
            except Exception as e:
                print(f"❌ Failed to start device {d.get('name', 'Unknown')}: {e}")
                # If barrier is broken, remaining devices will start independently
        

    def stop(self):
        print("Stopping Modbus service...")
        self._stop.set()
        for device_id, t in self._threads.items():
            t.join(timeout=2)  # Longer timeout for clean shutdown
        self._readers.clear()
        print("Modbus service stopped")

    def write_tag_value(self, tag_id: int, value: float) -> bool:
        """
        Global write function to write a value to any tag.
        Returns True if successful, False otherwise.
        """
        try:
            # Get tag info to find which device it belongs to
            tag = dbsync.get_tag(tag_id)
            if not tag:
                print(f"Tag {tag_id} not found")
                return False
                
            device_id = tag["device_id"]
            reader = self._readers.get(device_id)
            if not reader:
                print(f"No active reader for device {device_id}")
                return False
                
            return reader.write_tag_value(tag_id, value)
        except Exception as e:
            print(f"Error in global write function: {e}")
            return False

    def _get_device_interval(self, device_id: int) -> float:
        """DEPRECATED: Now using optimal intervals per device"""
        return 1.0

    def _device_loop(self, dev_row: Dict, reader: _DeviceReader):
        """DEPRECATED: Replaced by loop_with_timing for precision"""
        pass

