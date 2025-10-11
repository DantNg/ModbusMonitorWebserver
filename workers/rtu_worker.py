#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
RTU Worker - Handles Modbus RTU communication over serial port (reworked)
- Đọc block theo function code, map offset → giá trị từng tag
- Hỗ trợ datatype: Bit, UInt16/Int16, Float32, Int32/UInt32
- Poll theo interval riêng từng device (dù cùng 1 COM)
- Phát Socket.IO 'modbus_update' đúng format frontend
"""

import os
import sys
import time
import struct
import threading
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# ==== Optional deps ====
# DB
try:
    from shared.database_manager import DatabaseManager
    DB_AVAILABLE = True
except Exception:
    DB_AVAILABLE = False
    DatabaseManager = None
    print("⚠️  DatabaseManager not available. Running without DB writes.")

# Socket.IO
try:
    import socketio
    SIO_AVAILABLE = True
except Exception:
    SIO_AVAILABLE = False
    socketio = None
    print("⚠️  python-socketio not available. Skipping realtime emission.")

# pymodbus
try:
    from pymodbus.client import ModbusSerialClient
    PYMODBUS_AVAILABLE = True
except Exception:
    PYMODBUS_AVAILABLE = False
    ModbusSerialClient = None
    print("⚠️  pymodbus not available. Worker will not reach PLC (simulation only).")


# ==== Helpers ====
def now_hms():
    return datetime.now().strftime("%H:%M:%S")


def normalize_address(addr: int, base: str = "auto") -> int:
    """
    Chuẩn hoá địa chỉ về 0-based cho pymodbus.
    base:
      - 'zero' : DB đã 0-based
      - '10k'  : DB dạng 40001/30001/10001
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
    Lấy 1 giá trị từ block regs (0-based).
    Hỗ trợ: Word/UInt16, Int16, Float32, Int32, UInt32
    """
    dt = (datatype or "Word").strip().lower()

    # 1 word
    if dt in ("word", "uint16", "unsigned", "ushort"):
        return regs[offset]
    if dt in ("signed", "int16", "short"):
        v = regs[offset]
        return v if v < 32768 else v - 65536

    # 2 words
    if dt in ("float", "float32", "real", "ieee754"):
        if offset + 1 >= len(regs): return None
        hi, lo = (regs[offset], regs[offset + 1]) if word_order.upper() == "AB" else (regs[offset + 1], regs[offset])
        b = hi.to_bytes(2, "big") + lo.to_bytes(2, "big")
        return struct.unpack(">f", b)[0]

    if dt in ("int32", "dint", "long"):
        if offset + 1 >= len(regs): return None
        hi, lo = (regs[offset], regs[offset + 1]) if word_order.upper() == "AB" else (regs[offset + 1], regs[offset])
        val = (hi << 16) | lo
        return val if val < 2147483648 else val - 4294967296

    if dt in ("uint32", "dword"):
        if offset + 1 >= len(regs): return None
        hi, lo = (regs[offset], regs[offset + 1]) if word_order.upper() == "AB" else (regs[offset + 1], regs[offset])
        return (hi << 16) | lo

    return regs[offset]


# ==== Worker ====
class RTUWorker:
    def __init__(
        self,
        worker_id,
        serial_port,
        baudrate=9600,
        parity='N',
        stopbits=1,
        bytesize=8,
        timeout=1.5,
        devices=None,
        tags=None,
        webapp_url="http://127.0.0.1:5000",
        address_base="auto",
        debug=True,
    ):
        """
        devices: objects có .id, .name, .unit_id, .read_interval_ms [ms]
        tags:    objects có .id, .device_id, .name, .address, .function_code,
                 .data_type, .scale_factor, .offset, .unit, .word_order
        """
        self.worker_id = worker_id
        self.serial_port = serial_port
        self.baudrate = baudrate
        self.parity = parity
        self.stopbits = stopbits
        self.bytesize = bytesize
        self.timeout = timeout

        self.devices = devices or []
        self.tags = tags or []

        self.webapp_url = webapp_url
        self.address_base = address_base
        self.debug = debug

        # group tag theo device
        self.device_tags = {}
        for tg in self.tags:
            self.device_tags.setdefault(tg.device_id, []).append(tg)

        # DB
        self.db = DatabaseManager() if DB_AVAILABLE else None

        # Socket.IO
        self.sio = socketio.Client(reconnection=True) if SIO_AVAILABLE else None
        self.sio_connected = False

        # Modbus client (1 COM duy nhất cho worker này)
        self.client = None

        # runtime
        self.is_running = False
        self.thread = None

    # ---- lifecycle
    def start(self):
        if self.is_running:
            if self.debug: print("⚠️  RTU worker already running")
            return True

        if PYMODBUS_AVAILABLE:
            self.client = ModbusSerialClient(
                port=self.serial_port,
                baudrate=self.baudrate,
                parity=self.parity,
                stopbits=self.stopbits,
                bytesize=self.bytesize,
                timeout=self.timeout,
            )
            if not self.client.connect():
                print(f"❌ RTU connect fail {self.serial_port} @ {self.baudrate}")
                return False
            if self.debug: print(f"✅ Connected RTU {self.serial_port} @ {self.baudrate}")
        else:
            print("⚠️  pymodbus missing - no PLC access (simulation only)")

        if SIO_AVAILABLE:
            try:
                # Setup Socket.IO event handlers before connecting
                self._setup_socketio_handlers()
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
        print("🛑 RTU Worker stopped")

    # ---- Socket.IO Handlers ----
    def _setup_socketio_handlers(self):
        """Setup Socket.IO event handlers for write commands"""
        if not self.sio:
            return

        @self.sio.event
        def modbus_write_command(data):
            """Handle write command from webapp"""
            if self.debug:
                print(f"📝 Received write command: {data}")
            
            try:
                tag_id = data.get('tag_id')
                value = data.get('value')
                
                if tag_id is None or value is None:
                    self._send_write_response(tag_id, False, "Missing tag_id or value", data.get('frontend_client_id'))
                    return
                
                # Find tag by ID - only process if this worker manages this tag
                tag = None
                for t in self.tags:
                    if t.id == tag_id:
                        tag = t
                        break
                
                if not tag:
                    # This worker doesn't manage this tag - ignore silently
                    if self.debug:
                        print(f"⏭️  Tag {tag_id} not managed by this RTU worker, skipping")
                    return
                
                # Find device for this tag
                device = None
                for d in self.devices:
                    if d.id == tag.device_id:
                        device = d
                        break
                
                if not device:
                    self._send_write_response(tag_id, False, f"Device for tag {tag_id} not found", data.get('frontend_client_id'))
                    return
                
                # Perform write operation
                success, error = self._write_tag(device, tag, value)
                self._send_write_response(tag_id, success, error, data.get('frontend_client_id'))
                
            except Exception as e:
                error_msg = f"Write command error: {str(e)}"
                print(f"❌ {error_msg}")
                self._send_write_response(data.get('tag_id'), False, error_msg, data.get('frontend_client_id'))

    def _send_write_response(self, tag_id, success, error=None, frontend_client_id=None):
        """Send write response back to webapp"""
        if not self._ensure_sio():
            return
        
        response = {
            'tag_id': tag_id,
            'success': success,
            'error': error,
            'timestamp': datetime.now().isoformat(),
            'frontend_client_id': frontend_client_id  # For webapp to know which client to forward to
        }
        
        try:
            self.sio.emit('modbus_write_response', response)
            if self.debug:
                print(f"📤 Sent write response: {response}")
        except Exception as e:
            print(f"❌ Failed to send write response: {e}")

    def _write_tag(self, device, tag, value):
        """
        Write value to a single tag
        Returns: (success: bool, error_message: str)
        """
        if not PYMODBUS_AVAILABLE or not self.client:
            if self.debug:
                print(f"⚠️  Simulation: Would write {value} to tag {tag.name}")
            return True, None  # Simulate success for testing
        
        try:
            # Normalize address
            address = normalize_address(tag.address, self.address_base)
            unit = getattr(device, 'unit_id', 1)
            function_code = getattr(tag, 'function_code', 3)
            data_type = getattr(tag, 'data_type', 'Word')
            
            if self.debug:
                print(f"📝 Writing {value} to {tag.name} (addr={address}, unit={unit}, fc={function_code})")
            
            # Convert value based on data type
            write_value = self._prepare_write_value(value, data_type)
            
            # Write based on function code
            if function_code == 1:  # Coils
                result = self.client.write_coil(address, bool(write_value), slave=unit)
            elif function_code == 3:  # Holding Registers
                if data_type.lower() in ('float', 'float32', 'real', 'ieee754'):
                    # Convert float to two 16-bit registers
                    packed = struct.pack('>f', float(write_value))
                    reg1, reg2 = struct.unpack('>HH', packed)
                    word_order = getattr(tag, 'word_order', 'AB')
                    if word_order.upper() == 'AB':
                        registers = [reg1, reg2]
                    else:
                        registers = [reg2, reg1]
                    result = self.client.write_registers(address, registers, slave=unit)
                elif data_type.lower() in ('int32', 'uint32', 'dint', 'dword'):
                    # Convert 32-bit to two 16-bit registers
                    val = int(write_value)
                    if data_type.lower() in ('int32', 'dint'):
                        if val < 0:
                            val = val + 4294967296  # Convert to unsigned for transmission
                    reg1 = (val >> 16) & 0xFFFF
                    reg2 = val & 0xFFFF
                    word_order = getattr(tag, 'word_order', 'AB')
                    if word_order.upper() == 'AB':
                        registers = [reg1, reg2]
                    else:
                        registers = [reg2, reg1]
                    result = self.client.write_registers(address, registers, slave=unit)
                else:
                    # Single register (16-bit)
                    val = int(write_value)
                    if val < 0:
                        val = val + 65536  # Convert signed to unsigned
                    result = self.client.write_register(address, val, slave=unit)
            else:
                return False, f"Function code {function_code} not supported for writing"
            
            if result.isError():
                error_msg = f"Modbus write error: {result}"
                print(f"❌ {error_msg}")
                return False, error_msg
            
            if self.debug:
                print(f"✅ Successfully wrote {value} to {tag.name}")
            return True, None
            
        except Exception as e:
            error_msg = f"Write exception: {str(e)}"
            print(f"❌ {error_msg}")
            return False, error_msg

    def _prepare_write_value(self, value, data_type):
        """Prepare value for writing based on data type"""
        dt = (data_type or "Word").strip().lower()
        
        if dt in ("word", "uint16", "unsigned", "ushort"):
            return max(0, min(65535, int(value)))
        elif dt in ("signed", "int16", "short"):
            return max(-32768, min(32767, int(value)))
        elif dt in ("float", "float32", "real", "ieee754"):
            return float(value)
        elif dt in ("int32", "dint", "long"):
            return max(-2147483648, min(2147483647, int(value)))
        elif dt in ("uint32", "dword"):
            return max(0, min(4294967295, int(value)))
        else:
            return int(value)

    # ---- main loop with per-device schedule (cùng 1 COM)
    def _loop(self):
        if self.debug:
            print(f"🔄 RTU loop for {len(self.devices)} devices on {self.serial_port}")

        schedule_next = {}
        while self.is_running:
            now = time.time()
            for dev in self.devices:
                interval = max(0.1, getattr(dev, "read_interval_ms", 1000) / 1000.0)
                nxt = schedule_next.get(dev.id, 0)
                if now >= nxt:
                    try:
                        self._read_device(dev)
                    except Exception as e:
                        print(f"❌ read device {getattr(dev,'name',dev.id)}: {e}")
                    schedule_next[dev.id] = now + interval
            time.sleep(0.01)

    # ---- read one device (multi-block per FC)
    def _read_device(self, device):
        tags = self.device_tags.get(device.id, [])
        if not tags:
            return

        if self.debug:
            print(f"📖 Device {device.name} (Unit {getattr(device,'unit_id',1)}), tags: {len(tags)}")

        # group theo FC
        by_fc = {}
        for tg in tags:
            fc = getattr(tg, "function_code", 3) or 3
            by_fc.setdefault(fc, []).append(tg)

        all_rows = []
        success = 0

        for fc, tg_list in by_fc.items():
            block = self._read_block(device, fc, tg_list)
            if not block:
                continue

            for tg in tg_list:
                raw = block.get(tg.id)
                if raw is None:
                    continue

                val = float(raw)
                sf = getattr(tg, "scale_factor", 1.0) or 1.0
                off = getattr(tg, "offset", 0.0) or 0.0
                if sf != 1.0:
                    val *= sf
                if off != 0.0:
                    val += off

                success += 1

                # DB write (optional)
                if self.db:
                    try:
                        ts_full = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        self.db.update_tag_latest_value(tg.id, val, ts_full)
                    except Exception as e:
                        if self.debug: print(f"⚠️  DB update tag {tg.id} error: {e}")

                all_rows.append({
                    "id": tg.id,
                    "name": tg.name,
                    "value": val,
                    "datatype": getattr(tg, "data_type", "Word"),
                    "unit": getattr(tg, "unit", ""),
                    "ts": now_hms(),
                })

        if all_rows:
            self._emit_modbus_update(device, all_rows)

        # if self.debug:
        #     print(f"✅ {device.name}: {success}/{len(tags)} tags")

    # ---- block read + map offsets
    def _read_block(self, device, function_code, tags):
        """
        Trả về dict {tag_id: value}
        """
        # Simulation path
        if not PYMODBUS_AVAILABLE or not self.client:
            import random
            values = {}
            for tg in tags:
                if function_code in (1, 2):
                    values[tg.id] = random.choice([0, 1])
                else:
                    values[tg.id] = random.randint(100, 500)
            return values

        tags_sorted = sorted(tags, key=lambda t: t.address)
        min_addr = min(t.address for t in tags_sorted)
        max_addr = max(t.address for t in tags_sorted)

        start = normalize_address(min_addr, self.address_base)
        end = normalize_address(max_addr, self.address_base)
        count = end - start + 1
        unit = getattr(device, "unit_id", 1)

        if self.debug:
            print(f"   🔍 FC{function_code}: {min_addr}-{max_addr} → start={start}, count={count}, unit={unit}")

        try:
            if function_code == 1:
                res = self.client.read_coils(address=start, count=count, slave=unit)
            elif function_code == 2:
                res = self.client.read_discrete_inputs(address=start, count=count, slave=unit)
            elif function_code == 3:
                res = self.client.read_holding_registers(address=start, count=count, slave=unit)
            elif function_code == 4:
                res = self.client.read_input_registers(address=start, count=count, slave=unit)
            else:
                if self.debug: print(f"⚠️  Unsupported FC {function_code}")
                return {}
            if res.isError():
                if self.debug: print(f"❌ Modbus error FC{function_code}: {res}")
                return {}
        except Exception as e:
            print(f"❌ Modbus read exception FC{function_code}: {e}")
            return {}

        vals = {}
        if function_code in (1, 2):
            bits = getattr(res, "bits", [])
            for tg in tags_sorted:
                off = normalize_address(tg.address, self.address_base) - start
                try:
                    vals[tg.id] = 1 if bits[off] else 0
                except Exception:
                    vals[tg.id] = None
        else:
            regs = getattr(res, "registers", [])
            for tg in tags_sorted:
                off = normalize_address(tg.address, self.address_base) - start
                try:
                    v = unpack_from_registers(
                        regs, off,
                        getattr(tg, "data_type", "Word"),
                        getattr(tg, "word_order", "AB"),
                    )
                    vals[tg.id] = v
                except Exception:
                    vals[tg.id] = None

        return vals

    # ---- emit to webapp
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
            "latency_ms": 0,  # có thể đo và gán nếu bạn muốn
            "tags": tag_rows,
            "ts": now_hms(),
            # "room": f"subdashboard_{device.subdash_id}"  # nếu device có subdash_id
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
