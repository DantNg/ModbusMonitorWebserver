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
        self.timeout = (int(self.d.get("timeout_ms") or 1500))/1000.0
        self._connected = False
        self._backoff = 1.0
        self._next_retry_ts = 0.0

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

    def _read_registers(self, address: int, count: int):
        """Đọc holding registers từ thiết bị, xử lý ngoại lệ tốt hơn."""
        if count <= 0:
            raise ModbusIOException("count must be > 0")

        start_read_byte = self._normalize_hr_address(address)
        # response = self.client.read_holding_registers(start_read_byte, 10, slave=1)
        # print(response.registers)
        # print(f"Reading {count} registers from {self.d['name']} (ID: {self.d['id']}) at address {start_read_byte}")
        try:
            if self.d["protocol"] in ("ModbusTCP", "ModbusRTU"):
                if self.client != None:
                    # print(f"Using client: {self.client}")
                    pass
                else:
                    # print("Client is not connected, attempting to connect...")
                    if not self._ensure_connected():
                        raise ConnectionException("Failed to connect to Modbus client")
                rr = self.client.read_holding_registers(start_read_byte, count=count, slave=int(self.d["unit_id"]))
            else:
                raise ValueError("Unsupported protocol: {}".format(self.d["protocol"]))

            if rr is None or rr.isError():
                raise IOError("Read error: {}".format(rr))

            return rr.registers

        except (ConnectionException, ModbusIOException, IOError) as e:
            # print("Modbus read error:", e)
            self._close()
            # thay nan bằng None để MySQL không lỗi
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
        Ghi giá trị vào một tag cụ thể.
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
            
            # Áp dụng scale/offset ngược (từ giá trị thật về raw)
            scale = float(tag.get("scale") or 1.0)
            offset = float(tag.get("offset") or 0.0)
            raw_value = (value - offset) / scale if scale != 0 else value
            
            # Encode giá trị thành registers
            registers = self._encode_value_for_write(raw_value, tag["datatype"])
            
            # Tính địa chỉ ghi
            write_addr = self._normalize_hr_address(int(tag["address"]))
            
            # Thực hiện ghi
            if self.d["protocol"] in ("ModbusTCP", "ModbusRTU"):
                if len(registers) == 1:
                    # Ghi single register
                    result = self.client.write_register(write_addr, registers[0], slave=self.unit_id)
                else:
                    # Ghi multiple registers
                    result = self.client.write_registers(write_addr, registers, slave=self.unit_id)
                
                if result.isError():
                    print(f"Modbus write error for tag {tag['name']}: {result}")
                    return False
                    
                print(f"Successfully wrote value {value} to tag {tag['name']} (device: {self.d['name']})")
                return True
            else:
                print(f"Unsupported protocol for write: {self.d['protocol']}")
                return False
                
        except Exception as e:
            print(f"Error writing to tag {tag_id}: {e}")
            self._close()
            return False

    def loop_once(self):
        """Đọc 1 vòng cho device này - đọc bulk registers rồi parse từng tag."""

        if not self._ensure_connected():
            return  # chưa đến giờ retry, quay lại sau

        tags = dbsync.list_tags(self.d["id"])
        if not tags:
            return

        # Tính toán range cần đọc
        start_addr, count = self._calculate_read_range(tags)
        if count == 0:
            return

        # Đọc tất cả registers cần thiết một lần
        ts = utc_now()
        try:
            bulk_regs = self._read_registers(start_addr, count)
            if not bulk_regs or all(r is None for r in bulk_regs):
                self._close()
                return
                
            # Parse từng tag từ bulk data
            for t in tags:
                try:
                    addr = self._normalize_hr_address(int(t["address"]))
                    dt = t["datatype"]
                    scale = float(t.get("scale") or 1.0)
                    offs = float(t.get("offset") or 0.0)
                    
                    # Tính offset trong bulk_regs array
                    offset_in_bulk = addr - start_addr
                    
                    # Extract giá trị từ bulk data
                    val = self._extract(bulk_regs, offset_in_bulk, dt, scale, offs)
                    
                    # Lưu vào cache và queue
                    self.cache.set(int(t["id"]), ts, val)
                    self.dbq.put((int(t["id"]), ts, float(val)))
                    
                except Exception as e:
                    print(f"Error processing tag {t.get('name', 'unknown')}: {e}")
                    continue
            print(f"[{ts}] Device {self.d['name']} read {len(tags)} tags successfully.")    
        except Exception as e:
            print(f"Error reading bulk registers for device {self.d['name']}: {e}")
            self._close()


class ModbusService:
    """Mỗi device chạy trong 1 thread riêng, không block lẫn nhau"""
    def __init__(self, db_queue: Queue, push_queue: Queue, cache: LatestCache):
        self.dbq = db_queue
        self.pushq = push_queue
        self.cache = cache
        self._stop = threading.Event()
        self._threads: Dict[int, threading.Thread] = {}
        self._readers: Dict[int, _DeviceReader] = {}  # Store device readers for write access

    def start(self):
        devices = dbsync.list_devices()
        for d in devices:
            reader = _DeviceReader(d, db_queue=self.dbq, push_queue=self.pushq, cache=self.cache)
            self._readers[d["id"]] = reader  # Store reader reference
            t = threading.Thread(target=self._device_loop, args=(d, reader), daemon=True)
            t.start()
            self._threads[d["id"]] = t

    def stop(self):
        self._stop.set()
        for t in self._threads.values():
            t.join(timeout=1)
        self._readers.clear()

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
        tag_logger_map = dbsync.get_tag_logger_map(device_id)
        intervals = [v["interval_sec"] for v in tag_logger_map.values()]
        return min(intervals) if intervals else 1.0

    def _device_loop(self, dev_row: Dict, reader: _DeviceReader):
        interval = self._get_device_interval(dev_row["id"])
        print("Interval : ",interval)
        while not self._stop.is_set():
            try:
                reader.loop_once()
            except Exception as e:
                print(f"[Device {dev_row['name']}] Error: {e}")
            time.sleep(interval)

