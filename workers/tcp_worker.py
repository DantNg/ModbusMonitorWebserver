#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TCP Worker - Handles Modbus TCP communication over network (reworked)
- Đọc nhiều tag theo block để giảm round-trip
- Map chuẩn giá trị theo offset trong block
- Hỗ trợ datatype cơ bản (bit, uint16/int16, float32, int32/uint32)
- Lịch poll theo interval riêng từng device
- Phát Socket.IO payload đúng format (modbus_update)
"""

import os
import sys
import time
import struct
import threading
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# -------- Optional Dependencies --------
# DB
try:
    from shared.database_manager import DatabaseManager
    DB_AVAILABLE = True
except Exception:
    DB_AVAILABLE = False
    DatabaseManager = None
    print("⚠️  DatabaseManager not available. Running without DB updates.")

# Socket.IO client
try:
    import socketio
    SIO_AVAILABLE = True
except Exception:
    SIO_AVAILABLE = False
    socketio = None
    print("⚠️  python-socketio not available. Skipping realtime emission.")

# pymodbus
try:
    from pymodbus.client import ModbusTcpClient
    PYMODBUS_AVAILABLE = True
except Exception:
    PYMODBUS_AVAILABLE = False
    ModbusTcpClient = None
    print("⚠️  pymodbus not available. Worker will not connect to PLC (use simulation if desired).")


# ---------- Helpers ----------
def now_hms():
    return datetime.now().strftime("%H:%M:%S")


def normalize_address(addr: int, base: str = "auto") -> int:
    """
    Chuẩn hoá địa chỉ sang 0-based.
    base:
      - 'zero' : DB đã 0-based
      - '10k'  : DB lưu kiểu 1-based/10k (40001/30001/10001)
      - 'auto' : <10001 => 0-based, >=10001 => 10k
    """
    a = int(addr)
    if base == "zero":
        return a
    if base == "10k" or a >= 10001:
        if a >= 40001: return a - 40001
        if a >= 30001: return a - 30001
        if a >= 10001: return a - 10001
    return a


def unpack_from_registers(regs, offset, datatype="Word", word_order="AB"):
    """
    Lấy 1 giá trị từ mảng regs (đã đọc block, 0-based).
    Hỗ trợ: Word/UInt16, Int16, Float32, Int32, UInt32
    """
    dt = (datatype or "Word").strip().lower()

    # 16-bit
    if dt in ("word", "uint16", "unsigned", "ushort"):
        return regs[offset]
    if dt in ("signed", "int16", "short"):
        val = regs[offset]
        return val if val < 32768 else val - 65536

    # 32-bit float
    if dt in ("float", "float32", "real", "ieee754"):
        if offset + 1 >= len(regs):
            return None
        hi, lo = (regs[offset], regs[offset + 1]) if word_order == "ab" or word_order == "AB" else (regs[offset + 1],
                                                                                                   regs[offset])
        b = hi.to_bytes(2, "big") + lo.to_bytes(2, "big")
        return struct.unpack(">f", b)[0]

    # 32-bit signed
    if dt in ("int32", "dint", "long"):
        if offset + 1 >= len(regs):
            return None
        hi, lo = (regs[offset], regs[offset + 1]) if word_order == "ab" or word_order == "AB" else (regs[offset + 1],
                                                                                                   regs[offset])
        val = (hi << 16) | lo
        return val if val < 2147483648 else val - 4294967296

    # 32-bit unsigned
    if dt in ("uint32", "dword"):
        if offset + 1 >= len(regs):
            return None
        hi, lo = (regs[offset], regs[offset + 1]) if word_order == "ab" or word_order == "AB" else (regs[offset + 1],
                                                                                                   regs[offset])
        return (hi << 16) | lo

    # default → 16-bit unsigned
    return regs[offset]


# ---------- Worker ----------
class TCPWorker:
    def __init__(self, worker_id, host, port=502, timeout=2.0,
                 devices=None, tags=None, webapp_url="http://127.0.0.1:5000",
                 address_base="auto", debug=True):
        """
        devices: list các object có tối thiểu: id, name, unit_id, read_interval_ms
        tags: list các object có tối thiểu: id, device_id, name, address, function_code, data_type, scale_factor, offset, unit, word_order
        """
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.timeout = timeout
        self.webapp_url = webapp_url
        self.debug = debug
        self.address_base = address_base

        self.devices = devices or []
        self.tags = tags or []

        # group tag theo device
        self.device_tags = {}
        for tg in self.tags:
            self.device_tags.setdefault(tg.device_id, []).append(tg)

        # DB
        self.db = DatabaseManager() if DB_AVAILABLE else None

        # Socket.IO
        self.sio = socketio.Client(reconnection=True) if SIO_AVAILABLE else None
        self.sio_connected = False

        # Modbus client
        self.client = None

        # runtime
        self.is_running = False
        self.thread = None

    # ---- lifecycle
    def start(self):
        if self.is_running:
            if self.debug: print("⚠️  Worker already running")
            return True

        if PYMODBUS_AVAILABLE:
            self.client = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout)
            if not self.client.connect():
                print(f"❌ TCP connect fail {self.host}:{self.port}")
                return False
            if self.debug: print(f"✅ Connected TCP {self.host}:{self.port}")
        else:
            print("⚠️  pymodbus missing - cannot read from device")

        if SIO_AVAILABLE:
            try:
                self.sio.connect(self.webapp_url, wait=True)
                self.sio_connected = True
                if self.debug: print("✅ Socket.IO connected")
            except Exception as e:
                print(f"⚠️  Socket.IO connect failed: {e}")
                self.sio_connected = False

        self.is_running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        return True

    def stop(self):
        self.is_running = False
        try:
            if self.thread:
                self.thread.join(timeout=5)
        except Exception:
            pass
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass
        try:
            if self.sio and self.sio_connected:
                self.sio.disconnect()
        except Exception:
            pass
        print("🛑 Worker stopped")

    # ---- poll loop with per-device schedule
    def _loop(self):
        if self.debug:
            print(f"🔄 Worker loop for {len(self.devices)} devices")

        # lịch poll riêng cho từng device
        schedule_next = {}
        while self.is_running:
            now = time.time()
            for dev in self.devices:
                interval = max(0.1, getattr(dev, "read_interval_ms", 1000) / 1000.0)
                nxt = schedule_next.get(dev.id, 0)
                if now >= nxt:
                    t0 = time.time()
                    try:
                        self._read_device(dev)
                    except Exception as e:
                        print(f"❌ read device {getattr(dev,'name',dev.id)}: {e}")
                    t1 = time.time()
                    # giữa hai lần poll tiếp theo vẫn giữ interval gốc
                    schedule_next[dev.id] = now + interval
            time.sleep(0.01)

    # ---- read 1 device
    def _read_device(self, device):
        tags = self.device_tags.get(device.id, [])
        if not tags:
            return

        if self.debug:
            print(f"📖 Device {device.name} (Unit {getattr(device,'unit_id',1)}), tags: {len(tags)}")

        # group theo function code
        by_fc = {}
        for tg in tags:
            fc = getattr(tg, "function_code", 3) or 3
            by_fc.setdefault(fc, []).append(tg)

        all_tag_rows = []
        successful = 0

        for fc, tg_list in by_fc.items():
            block_values = self._read_block(device, fc, tg_list)
            if not block_values:
                continue

            for tg in tg_list:
                raw = block_values.get(tg.id)
                if raw is None:
                    continue

                val = float(raw)
                sf = getattr(tg, "scale_factor", 1.0) or 1.0
                off = getattr(tg, "offset", 0.0) or 0.0
                if sf != 1.0:
                    val *= sf
                if off != 0.0:
                    val += off

                successful += 1

                # lưu DB (nếu có)
                if self.db:
                    try:
                        ts_full = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        self.db.update_tag_latest_value(tg.id, val, ts_full)
                    except Exception as e:
                        if self.debug: print(f"⚠️  DB update tag {tg.id} error: {e}")

                # gom để emit
                all_tag_rows.append({
                    "id": tg.id,
                    "name": tg.name,
                    "value": val,
                    "datatype": getattr(tg, "data_type", "Word"),
                    "unit": getattr(tg, "unit", ""),
                    "ts": now_hms(),
                })

        if all_tag_rows:
            self._emit_modbus_update(device, all_tag_rows)

        # if self.debug:
        #     print(f"✅ {device.name}: {successful}/{len(tags)} tags")

    # ---- block read + mapping
    def _read_block(self, device, function_code, tags):
        """
        return: dict {tag_id: value}
        """
        if not self.client:
            return {}

        # sort theo address
        tags_sorted = sorted(tags, key=lambda t: t.address)
        min_addr = min(t.address for t in tags_sorted)
        max_addr = max(t.address for t in tags_sorted)

        start = normalize_address(min_addr, self.address_base)
        end = normalize_address(max_addr, self.address_base)
        count = end - start + 1
        unit = getattr(device, "unit_id", 1)

        if self.debug:
            print(f"   🔍 FC{function_code}: {min_addr}-{max_addr} → start={start}, count={count}")

        # thực hiện đọc
        try:
            if function_code == 1:
                result = self.client.read_coils(address=start, count=count, slave=unit)
            elif function_code == 2:
                result = self.client.read_discrete_inputs(address=start, count=count, slave=unit)
            elif function_code == 3:
                result = self.client.read_holding_registers(address=start, count=count, slave=unit)
            elif function_code == 4:
                result = self.client.read_input_registers(address=start, count=count, slave=unit)
            else:
                if self.debug: print(f"⚠️  Unsupported FC {function_code}")
                return {}
            if result.isError():
                if self.debug: print(f"❌ Modbus error FC{function_code}: {result}")
                return {}
        except Exception as e:
            print(f"❌ Modbus read exception FC{function_code}: {e}")
            return {}

        values = {}
        if function_code in (1, 2):
            # bits
            bits = getattr(result, "bits", [])
            for tg in tags_sorted:
                off = normalize_address(tg.address, self.address_base) - start
                try:
                    values[tg.id] = 1 if bits[off] else 0
                except Exception:
                    values[tg.id] = None
        else:
            # registers
            regs = getattr(result, "registers", [])
            for tg in tags_sorted:
                off = normalize_address(tg.address, self.address_base) - start
                try:
                    v = unpack_from_registers(
                        regs, off,
                        getattr(tg, "data_type", "Word"),
                        getattr(tg, "word_order", "AB"),
                    )
                    values[tg.id] = v
                except Exception:
                    values[tg.id] = None

        return values

    # ---- emit Socket.IO
    def _ensure_sio(self):
        if not SIO_AVAILABLE:
            return False
        if self.sio and self.sio_connected:
            return True
        try:
            if not self.sio:
                self.sio = socketio.Client(reconnection=True)
            self.sio.connect(self.webapp_url, wait=True)
            self.sio_connected = True
            return True
        except Exception as e:
            print(f"⚠️  Socket.IO reconnect failed: {e}")
            self.sio_connected = False
            return False

    def _emit_modbus_update(self, device, tag_rows):
        payload = {
            "device_id": f"dev{device.id}",
            "device_name": device.name,
            "unit": getattr(device, "unit_id", 1),
            "ok": True,
            "seq": int(time.time()),
            "latency_ms": 0,  # nếu cần, bạn đo thời gian đọc block để gán
            "tags": tag_rows,
            "ts": now_hms(),
            # "room": f"subdashboard_{device.subdash_id}"  # nếu có subdash id trên device
        }

        if self._ensure_sio():
            try:
                self.sio.emit("modbus_update", payload)
                if self.debug:
                    print(f"   📡 Emitted {len(tag_rows)} tags for {device.name}")
            except Exception as e:
                print(f"⚠️  emit failed: {e}")
                self.sio_connected = False
        else:
            if self.debug:
                print("⚠️  Socket.IO unavailable; payload not sent")


# ---------- Example usage ----------
if __name__ == "__main__":
    # Bạn có thể gắn list devices/tags từ DB trước khi start worker.
    # Tối thiểu:
    # device: obj có .id, .name, .unit_id, .read_interval_ms
    # tag:    obj có .id, .device_id, .name, .address, .function_code, .data_type, .scale_factor, .offset, .unit
    print("This file is a library for TCP Modbus worker. Import and instantiate TCPWorker in your runner.")
