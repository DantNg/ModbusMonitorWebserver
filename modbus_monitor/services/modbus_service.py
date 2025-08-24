from __future__ import annotations
import threading, time, math, os
from queue import Queue
from typing import List, Dict, Tuple
from datetime import datetime
from modbus_monitor.database import db as dbsync
from modbus_monitor.services.common import LatestCache, utc_now
from pymodbus.exceptions import ModbusIOException

import struct
# pymodbus sync
from pymodbus.client import ModbusTcpClient, ModbusSerialClient
from pymodbus.exceptions import ModbusIOException
from pymodbus.exceptions import ConnectionException
def _apply_sf(raw: float, scale: float, offset: float) -> float:
    return raw * (scale or 1.0) + (offset or 0.0)

def _unpack_u32(lo: int, hi: int, word_order: str) -> int:
    return ((hi << 16) | lo) if (word_order or "AB") == "AB" else ((lo << 16) | hi)

def _unpack_float(lo: int, hi: int, byte_order: str, word_order: str) -> float:
    import struct
    w1, w2 = (hi, lo) if (word_order or "AB") == "AB" else (lo, hi)
    b1 = w1.to_bytes(2, "big")
    b2 = w2.to_bytes(2, "big")
    b = b1 + b2
    if (byte_order or "BigEndian") == "LittleEndian":
        b = b[1:2] + b[0:1] + b[3:4] + b[2:3]
    return struct.unpack(">f", b)[0]

class _DeviceReader:
    def __init__(self, dev_row: Dict, db_queue: Queue, cache: LatestCache):
        self.d = dev_row
        self.dbq = db_queue
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
        if not ok:
            self._backoff = min(self._backoff * 2.0, 20.0)
            self._next_retry_ts = now + min(self._backoff, 5.0)
        else:
            self._backoff = 1.0
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
                rr = self.client.read_holding_registers(start_read_byte, count, slave=int(self.d["unit_id"]))
            else:
                raise ValueError("Unsupported protocol: {}".format(self.d["protocol"]))

            if rr is None or rr.isError():
                raise IOError("Read error: {}".format(rr))

            return rr.registers

        except (ConnectionException, ModbusIOException, IOError) as e:
            print("Modbus read error:", e)
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

        # --- helpers ---
        def _two_words():
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
                if offset + 1 >= len(regs): return math.nan
                lo, hi, _ = _two_words()
                u32 = (hi << 16) | lo if (self.word_order or "AB") == "AB" else (lo << 16) | hi
                val = float(u32)

            # 32-bit signed
            elif name in ("dint", "int32", "int"):
                if offset + 1 >= len(regs): return math.nan
                lo, hi, _ = _two_words()
                u32 = (hi << 16) | lo if (self.word_order or "AB") == "AB" else (lo << 16) | hi
                # chuyển sang signed
                if u32 & 0x80000000:
                    u32 = -((~u32 & 0xFFFFFFFF) + 1)
                val = float(u32)

            # 32-bit float (IEEE754)
            elif name in ("float", "float32", "real"):
                if offset + 1 >= len(regs): return math.nan
                _, _, b = _two_words()
                val = float(struct.unpack(">f", b)[0])

            # Bit / boolean
            elif name in ("bit", "bool", "boolean"):
                val = 1.0 if regs[offset] != 0 else 0.0

            else:
                # Datatype chưa biết → trả NaN để UI thấy rõ
                return math.nan

            return val * (scale or 1.0) + (offs or 0.0)

        except Exception:
            return math.nan
    def loop_once(self):
        """Đọc 1 vòng ngắn cho device này rồi return (KHÔNG block mãi)."""

        if not self._ensure_connected():
            return  # chưa đến giờ retry, quay lại sau

        tags = dbsync.list_tags(self.d["id"])
        # print(tags)
        if not tags:
            return

        ts = utc_now()
        for t in tags:
            addr = int(t["address"])
            dt   = t["datatype"]
            scale = float(t.get("scale") or 1.0)
            offs  = float(t.get("offset") or 0.0)
            count = 1 if dt == "Word" else 2
            # print(f"Reading tag {t['name']} (ID: {t['id']}) at address {addr} with datatype {dt}")
            regs = self._read_registers(addr, count)
            val = self._extract(regs, 0, dt, scale, offs)
            self.cache.set(int(t["id"]), ts, val)
            self.dbq.put((int(t["id"]), ts, float(val)))
            # print("v")
            # print(f"Tag {t['name']} (ID: {t['id']}) value: {val} at {ts}")
            try:
                pass
            except Exception:
                # lỗi đọc → đóng để lần sau retry
                self._close()
                break

class ModbusService:
    """Hai thread: 1 cho TCP, 1 cho RTU. Mỗi thread đi vòng qua các device cùng loại."""
    def __init__(self, db_queue: Queue, cache: LatestCache):
        self.dbq = db_queue
        self.cache = cache
        self._stop = threading.Event()
        self._t_tcp = threading.Thread(target=self._tcp_loop, name="modbus-tcp", daemon=True)
        self._t_rtu = threading.Thread(target=self._rtu_loop, name="modbus-rtu", daemon=True)
        self._next_due: Dict[int, float] = {}  # device_id -> next_due timestamp

    def start(self):
        self._t_tcp.start()
        self._t_rtu.start()

    def stop(self):
        self._stop.set()
        self._t_tcp.join(timeout=3)
        self._t_rtu.join(timeout=3)
        
    def _get_device_interval(self, device_id: int) -> float:
        tag_logger_map = dbsync.get_tag_logger_map(device_id)
        intervals = [v["interval_sec"] for v in tag_logger_map.values()]
        return min(intervals) if intervals else 1

    def _poll_loop(self, protocol: str):
        readers: List[_DeviceReader] = []
        while not self._stop.is_set():
            devs = [d for d in dbsync.list_devices() if d["protocol"] == protocol]
            readers = [_DeviceReader(d, self.dbq, self.cache) for d in devs]
            now = time.time()
            for r in readers:
                did = r.d["id"]
                interval = self._get_device_interval(did)
                due = self._next_due.get(did, now)
                if now >= due:
                    r.loop_once()
                    self._next_due[did] = now + interval
            time.sleep(0.2)
            
    def _tcp_loop(self):
        self._poll_loop("ModbusTCP")

    def _rtu_loop(self):
        pass
        self._poll_loop("ModbusRTU")
