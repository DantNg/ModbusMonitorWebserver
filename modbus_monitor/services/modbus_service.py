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
        self.timeout = min((int(self.d.get("timeout_ms") or 500))/1000.0, 0.5)  # Shorter timeout for speed
        self._connected = False
        self._backoff = 1.0
        self._next_retry_ts = 0.0
        self._seq = 0  # Sequence counter for debugging
        self._device_id_str = f"dev{self.d['id']}"  # String ID for socket emissions

    def _connect(self) -> bool:
        # if os.getenv("FAKE_MODBUS") == "1":
        #     self.client = "FAKE"
        #     return True

        if self.d["protocol"] == "ModbusTCP":
            # print("Connecting to ModbusTCP with host {}".format(self.d.get("host")))
            self.client = ModbusTcpClient(self.d.get("host"), port=int(self.d.get("port") or 502))
        else:
            self.client = ModbusSerialClient(
                port=self.d.get("serial_port"),
                baudrate=int(self.d.get("baudrate") or 9600),
                bytesize=int(self.d.get("bytesize") or 8),
                parity=(self.d.get("parity") or "N"),
                stopbits=int(self.d.get("stopbits") or 1),
                timeout=self.timeout
            )
        return self.client.connect()

    def _ensure_connected(self) -> bool:
        now = time.time()
        if self._connected:
            return True
        if now < self._next_retry_ts:
            return False
        ok = self._connect()
        if ok:
            self._connected = True
            self._backoff = 1.0
        else:
            self._backoff = min(self._backoff * 2.0, 20.0)
            self._next_retry_ts = now + min(self._backoff, 5.0)
        return ok

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
                    # Convert boolean bits to integers for consistency
                    return [int(bit) for bit in rr.bits[:count]] if not rr.isError() else None
                elif function_code == 2:  # Read Discrete Inputs
                    rr = self.client.read_discrete_inputs(start_read_byte, count=count, slave=int(self.d["unit_id"]))
                    # Convert boolean bits to integers for consistency
                    return [int(bit) for bit in rr.bits[:count]] if not rr.isError() else None
                elif function_code == 3:  # Read Holding Registers
                    rr = self.client.read_holding_registers(start_read_byte, count=count, slave=int(self.d["unit_id"]))
                elif function_code == 4:  # Read Input Registers
                    rr = self.client.read_input_registers(start_read_byte, count=count, slave=int(self.d["unit_id"]))
                else:
                    raise ValueError(f"Unsupported function code: {function_code}")
                    
            else:
                raise ValueError("Unsupported protocol: {}".format(self.d["protocol"]))

            if rr is None or rr.isError():
                raise IOError("Read error: {}".format(rr))

            # For register-based functions (3, 4), return registers
            if function_code in [3, 4]:
                return rr.registers
            # For bit-based functions (1, 2), already converted above
            else:
                return rr

        except (ConnectionException, ModbusIOException, IOError) as e:
            # print("Modbus read error:", e)
            self._close()
            # Return None values to indicate read failure
            return [None] * count  

        except Exception as e:
            print("Unexpected error during Modbus read:", e)
            self._close()
            return [None] * count

    def _extract(self, regs: list[int], offset: int, datatype: str, scale: float, offs: float) -> float:
        """
        Chuyển regs -> giá trị thật theo datatype.
        Hỗ trợ alias: word/uint16/ushort, short/int16, dword/uint32/udint, dint/int32/int,
                    float/real, bit/bool/boolean.
        Tôn trọng self.word_order ('AB'|'BA') và self.byte_order ('BigEndian'|'LittleEndian').
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

        try:
            # 16-bit unsigned
            if name in ("word", "uint16", "ushort"):
                val = float(regs[offset])

            # 16-bit signed (big-endian word)
            elif name in ("short", "int16"):
                val = float(struct.unpack(">h", struct.pack(">H", regs[offset]))[0])

            # 32-bit unsigned
            elif name in ("dword", "uint32", "udint"):
                lo, hi, b = _two_words()
                if lo is None or hi is None:
                    return math.nan
                u32 = (hi << 16) | lo if (self.word_order or "AB") == "AB" else (lo << 16) | hi
                val = float(u32)

            # 32-bit signed
            elif name in ("dint", "int32", "int"):
                lo, hi, b = _two_words()
                if lo is None or hi is None:
                    return math.nan
                u32 = (hi << 16) | lo if (self.word_order or "AB") == "AB" else (lo << 16) | hi
                # chuyển sang signed
                if u32 & 0x80000000:
                    u32 = -((~u32 & 0xFFFFFFFF) + 1)
                val = float(u32)

            # 32-bit float (IEEE754)
            elif name in ("float", "float32", "real"):
                lo, hi, b = _two_words()
                if b is None:
                    return math.nan
                val = float(struct.unpack(">f", b)[0])

            # Bit / boolean
            elif name in ("bit", "bool", "boolean"):
                val = 1.0 if regs[offset] != 0 else 0.0

            else:
                # Datatype chưa biết → trả NaN để UI thấy rõ
                return math.nan

            val = val * (scale or 1.0) + (offs or 0.0)
            # Giới hạn số số 0 sau dấu phẩy (làm tròn đến 6 chữ số thập phân, loại bỏ số 0 dư)
            return float(f"{val:.2f}".rstrip('0').rstrip('.') if '.' in f"{val:.2f}" else f"{val:.2f}")

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
        Tôn trọng self.word_order ('AB'|'BA') và self.byte_order ('BigEndian'|'LittleEndian').
        """
        name = (datatype or "").strip().lower()
        
        try:
            if name in ("word", "uint16", "ushort"):
                # 16-bit unsigned
                val = int(value) & 0xFFFF
                return [val]
                
            elif name in ("short", "int16"):
                # 16-bit signed
                val = int(value)
                if val < 0:
                    val = (1 << 16) + val  # Two's complement
                return [val & 0xFFFF]
                
            elif name in ("dword", "uint32", "udint"):
                # 32-bit unsigned
                val = int(value) & 0xFFFFFFFF
                lo = val & 0xFFFF
                hi = (val >> 16) & 0xFFFF
                # word order: AB = hi->lo, BA = lo->hi
                return [hi, lo] if (self.word_order or "AB") == "AB" else [lo, hi]
                
            elif name in ("dint", "int32", "int"):
                # 32-bit signed
                val = int(value)
                if val < 0:
                    val = (1 << 32) + val  # Two's complement
                val = val & 0xFFFFFFFF
                lo = val & 0xFFFF
                hi = (val >> 16) & 0xFFFF
                return [hi, lo] if (self.word_order or "AB") == "AB" else [lo, hi]
                
            elif name in ("float", "float32", "real"):
                # 32-bit float (IEEE754)
                import struct
                packed = struct.pack('>f', float(value))
                w1, w2 = struct.unpack('>HH', packed)
                
                # Áp dụng byte order trong từng word
                if (self.byte_order or "BigEndian") == "LittleEndian":
                    w1 = ((w1 & 0xFF) << 8) | ((w1 >> 8) & 0xFF)
                    w2 = ((w2 & 0xFF) << 8) | ((w2 >> 8) & 0xFF)
                
                # word order: AB = w1->w2, BA = w2->w1
                return [w1, w2] if (self.word_order or "AB") == "AB" else [w2, w1]
                
            elif name in ("bit", "bool", "boolean"):
                # Boolean
                return [1 if value else 0]
                
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
            return  # chưa đến giờ retry, quay lại sau

        tags = dbsync.list_tags(self.d["id"])
        if not tags:
            return

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
                        if val is not None:
                            self.dbq.put((int(t["id"]), ts, float(val)))
                            # Track for immediate emission
                            all_successful_tags.append({
                                "id": int(t["id"]),
                                "name": t.get("name", "tag_test"),
                                "value": float(val),
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
            
            # Emit to specific device dashboard view
            socketio.emit("modbus_update", {
                "device_id": self._device_id_str,
                "device_name": self.d.get("name", "Unknown"),
                "unit": self.d.get("unit_id", 1),
                "ok": True,
                "tags": all_successful_tags,
                "seq": self._seq,
                "latency_ms": latency_ms,
                "ts": time.time(),
                "tag_count": len(all_successful_tags)
            }, room=f"dashboard_device_{self.d['id']}")
            
            # Also emit to "all devices" dashboard view
            socketio.emit("modbus_update", {
                "device_id": self._device_id_str,
                "device_name": self.d.get("name", "Unknown"),
                "unit": self.d.get("unit_id", 1),
                "ok": True,
                "tags": all_successful_tags,
                "seq": self._seq,
                "latency_ms": latency_ms,
                "ts": time.time(),
                "tag_count": len(all_successful_tags)
            }, room="dashboard_all")
            
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
                
        # print(f"[{ts}] Device {self.d['name']} processed {len(tags)} tags in {latency_ms}ms (seq: {self._seq})")

    def loop_with_timing(self, start_epoch: float, barrier: threading.Barrier):
        """
        High-precision loop with barrier synchronization and anti-drift timing.
        Based on async_modbus.py approach.
        """
        # Connect first
        if not self._ensure_connected():
            print(f"Device {self.d['name']}: Failed initial connection")
            return
            
        try:
            # Wait for all devices to be ready
            barrier.wait()
            
            # Wait until the synchronized start time
            now = time.monotonic()
            if now < start_epoch:
                time.sleep(start_epoch - now)
            
            interval = max(self._get_optimal_interval(), 0.5)  # Minimum 500ms
            next_run = start_epoch
            
            while True:  # Run indefinitely until thread is stopped
                now = time.monotonic()
                if now < next_run:
                    time.sleep(next_run - now)
                
                # Execute one read cycle
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
            # Use minimum interval but cap between 0.5-2.0 seconds for real-time
            min_interval = min(intervals) if intervals else 1.0
            return max(min(min_interval, 2.0), 0.5)
        except Exception:
            return 1.0  # Default 1 second


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
        
        for d in devices:
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
            
        print(f"All {len(devices)} device readers started with precision timing")

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

