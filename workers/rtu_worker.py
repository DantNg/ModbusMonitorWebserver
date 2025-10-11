#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RTU Worker - Modbus RTU over serial (pymodbus v2/v3 compatible: unit/slave auto-detect)
- Block-read theo function code, map offset từng tag
- Hỗ trợ write (coil, holding register 1/2/4 regs) qua Socket.IO
- Dùng converter riêng: convert_raw_value_to_web / convert_web_value_to_raw / get_register_count
"""

import os
import sys
import time
import struct
import inspect
import threading
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))

# ---- Optional: DB
try:
    from shared.database_manager import DatabaseManager
    DB_AVAILABLE = True
except Exception:
    DB_AVAILABLE = False
    DatabaseManager = None
    print("⚠️  DatabaseManager not available. Running without DB writes.")

# ---- Optional: Socket.IO
try:
    import socketio
    SIO_AVAILABLE = True
except Exception:
    SIO_AVAILABLE = False
    socketio = None
    print("⚠️  python-socketio not available. Skipping realtime emission.")

# ---- pymodbus
try:
    from pymodbus.client import ModbusSerialClient
    PYMODBUS_AVAILABLE = True
except Exception:
    PYMODBUS_AVAILABLE = False
    ModbusSerialClient = None
    print("⚠️  pymodbus not available. Worker will run in no-PLC mode.")

# ---- value converter (bạn đã có sẵn)
try:
    # Try multiple import paths
    try:
        from utils.value_converter import (
            convert_raw_value_to_web,
            convert_web_value_to_raw,
            get_register_count,
        )
    except ImportError:
        # Try relative import
        import sys
        import os
        utils_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils')
        sys.path.insert(0, utils_path)
        from value_converter import (
            convert_raw_value_to_web,
            convert_web_value_to_raw,
            get_register_count,
        )
    VC_AVAILABLE = True
except Exception as e:
    VC_AVAILABLE = False
    print(f"⚠️  value_converter not found ({e}). Using minimal unpackers as fallback.")

# ---------- Helpers ----------
def now_hms():
    return datetime.now().strftime("%H:%M:%S")

def normalize_address(addr: int, base: str = "auto") -> int:
    a = int(addr)
    if base == "zero":
        return a
    if base == "10k" or a >= 10001:
        if a >= 40001: return a - 40001
        if a >= 30001: return a - 30001
        if a >= 10001: return a - 10001
    return a

def _fallback_unpack(regs, offset, datatype="Word", byte_order="BigEndian", word_order="AB"):
    dt = (datatype or "Word").lower()
    if dt in ("word", "uint16", "unsigned", "ushort"): return regs[offset]
    if dt in ("signed", "int16", "short"):
        v = regs[offset]
        return v if v < 32768 else v - 65536
    if dt in ("float","float32","real","ieee754"):
        if offset + 1 >= len(regs): return None
        hi, lo = (regs[offset], regs[offset+1]) if word_order.upper()=="AB" else (regs[offset+1], regs[offset])
        b = hi.to_bytes(2,"big")+lo.to_bytes(2,"big")
        if byte_order=="LittleEndian": b = b[1:2]+b[0:1]+b[3:4]+b[2:3]
        return struct.unpack(">f", b)[0]
    if dt in ("int32","dint","long"):
        if offset + 1 >= len(regs): return None
        hi, lo = (regs[offset], regs[offset+1]) if word_order.upper()=="AB" else (regs[offset+1], regs[offset])
        val = (hi<<16)|lo
        return val if val<2147483648 else val-4294967296
    if dt in ("uint32","dword"):
        if offset + 1 >= len(regs): return None
        hi, lo = (regs[offset], regs[offset+1]) if word_order.upper()=="AB" else (regs[offset+1], regs[offset])
        return (hi<<16)|lo
    return regs[offset]

# ---------- Worker ----------
class RTUWorker:
    def __init__(self, worker_id, serial_port, baudrate=9600, timeout=1.0,
                 devices=None, tags=None, webapp_url="http://127.0.0.1:5000",
                 address_base="auto", parity='N', stopbits=1, bytesize=8, debug=True):
        self.worker_id = worker_id
        self.serial_port = serial_port
        self.baudrate = baudrate
        self.timeout = timeout
        self.parity = parity
        self.stopbits = stopbits
        self.bytesize = bytesize
        self.webapp_url = webapp_url
        self.debug = debug
        self.address_base = address_base

        self.devices = devices or []
        self.tags = tags or []

        self.device_tags = {}
        for tg in self.tags:
            self.device_tags.setdefault(tg.device_id, []).append(tg)

        self.db = DatabaseManager() if DB_AVAILABLE else None

        self.sio = socketio.Client(reconnection=True) if SIO_AVAILABLE else None
        self.sio_connected = False

        self.client = None
        self._unit_kw = "unit"

        self.is_running = False
        self.thread = None

    def start(self):
        if self.is_running:
            if self.debug: print("⚠️  RTUWorker already running"); return True

        if PYMODBUS_AVAILABLE:
            self.client = ModbusSerialClient(
                port=self.serial_port,
                baudrate=self.baudrate,
                timeout=self.timeout,
                parity=self.parity,
                stopbits=self.stopbits,
                bytesize=self.bytesize
            )
            if not self.client.connect():
                print(f"❌ RTU connect fail {self.serial_port}")
                return False
            self._unit_kw = self._detect_unit_keyword()
            if self.debug:
                print(f"✅ Connected RTU {self.serial_port}@{self.baudrate} (kw='{self._unit_kw}')")
        else:
            print("⚠️  pymodbus missing - cannot talk to serial device")

        if SIO_AVAILABLE:
            try:
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
            if self.thread: self.thread.join(timeout=5)
        except Exception: pass
        try:
            if self.client: self.client.close()
        except Exception: pass
        try:
            if self.sio and self.sio_connected: self.sio.disconnect()
        except Exception: pass
        print("🛑 RTUWorker stopped")

    def _detect_unit_keyword(self):
        try:
            sig = inspect.signature(self.client.read_holding_registers)
            params = sig.parameters
            if "unit" in params: return "unit"
            if "slave" in params: return "slave"
        except Exception:
            pass
        return "unit"

    def _call_modbus(self, func, /, unit_val=1, **kwargs):
        kwargs[self._unit_kw] = unit_val
        try:
            return func(**kwargs)
        except TypeError:
            alt_kw = "slave" if self._unit_kw == "unit" else "unit"
            try:
                kwargs.pop(self._unit_kw, None)
                kwargs[alt_kw] = unit_val
                self._unit_kw = alt_kw
                return func(**kwargs)
            except TypeError as e:
                raise e

    def _setup_socketio_handlers(self):
        if not self.sio: return

        @self.sio.on("connect")
        def on_connect():
            self.sio_connected = True
            if self.debug:
                print("📡 Socket.IO connected")

        @self.sio.on("disconnect")
        def on_disconnect():
            self.sio_connected = False
            if self.debug:
                print("📡 Socket.IO disconnected")

        @self.sio.on("modbus_write_command")
        def _on_write_command(data):
            if self.debug: 
                print(f"📝 Write command: {data}")
            
            # Validate input data first
            if not data or not isinstance(data, dict):
                print(f"❌ Invalid write command data: {data}")
                return
                
            try:
                tag_id = data.get('tag_id')
                value = data.get('value')
                if tag_id is None or value is None:
                    self._send_write_response(tag_id, False, "Missing tag_id/value", data.get('frontend_client_id'))
                    return
                    
                tag = next((t for t in self.tags if t.id == tag_id), None)
                if not tag:
                    if self.debug: 
                        print(f"⏭️ Tag {tag_id} không thuộc worker này")
                    return
                    
                # Safely access tag.device_id
                device_id = getattr(tag, 'device_id', None)
                if device_id is None:
                    self._send_write_response(tag_id, False, "Tag missing device_id", data.get('frontend_client_id'))
                    return
                    
                device = next((d for d in self.devices if d.id == device_id), None)
                if not device:
                    self._send_write_response(tag_id, False, "Device not found", data.get('frontend_client_id'))
                    return
                    
                ok, err = self._write_tag(device, tag, value)
                self._send_write_response(tag_id, ok, err, data.get('frontend_client_id'))
            except Exception as e:
                # Safe error handling
                tag_id = data.get('tag_id') if data and isinstance(data, dict) else None
                frontend_client_id = data.get('frontend_client_id') if data and isinstance(data, dict) else None
                self._send_write_response(tag_id, False, f"Write error: {e}", frontend_client_id)

    def _send_write_response(self, tag_id, success, error=None, frontend_client_id=None):
        if not self._ensure_sio(): return
        try:
            self.sio.emit("modbus_write_response", {
                "tag_id": tag_id,
                "success": success,
                "error": error,
                "timestamp": datetime.now().isoformat(),
                "frontend_client_id": frontend_client_id
            })
            if self.debug:
                print(f"📤 write_response: tag={tag_id}, ok={success}, err={error}")
        except Exception as e:
            print(f"❌ write_response emit failed: {e}")

    def _write_tag(self, device, tag, value):
        if not PYMODBUS_AVAILABLE or not self.client:
            if self.debug: print(f"⚠️ simulate write {value} -> {tag.name}")
            return True, None
        try:
            unit = getattr(device, "unit_id", 1)
            addr = normalize_address(int(tag.address), self.address_base)
            fc   = int(getattr(tag, "function_code", 3) or 3)
            dtype = getattr(tag, "data_type", None) or getattr(tag, "datatype", "Word")
            byte_order = getattr(device, "byte_order", "BigEndian")
            word_order = getattr(tag, "word_order", getattr(device, "word_order", "AB"))

            # ===== REVERSE SCALE/OFFSET =====
            # Khi đọc: displayed_value = (raw_value * scale) + offset
            # Khi ghi: raw_value = (displayed_value - offset) / scale
            scale_factor = getattr(tag, "scale_factor", 1.0) or 1.0
            offset = getattr(tag, "offset", 0.0) or 0.0
            
            # Convert displayed value back to raw value
            raw_value = float(value)
            if offset != 0.0:
                raw_value -= offset
            if scale_factor != 1.0:
                raw_value /= scale_factor
                
            if self.debug:
                print(f"📝 Write conversion: {value} -> {raw_value} (offset={offset}, scale={scale_factor})")

            # ⚠️ SỬA: Dùng raw_value thay vì value
            if VC_AVAILABLE:
                raw_regs = convert_web_value_to_raw(raw_value, dtype, byte_order, word_order)
            else:
                raw_regs = [int(raw_value)]

            if fc == 1:
                res = self._call_modbus(self.client.write_coil, address=addr, value=bool(raw_regs[0]), unit_val=unit)
            elif fc == 3:
                if len(raw_regs) == 1:
                    res = self._call_modbus(self.client.write_register, address=addr, value=raw_regs[0], unit_val=unit)
                else:
                    res = self._call_modbus(self.client.write_registers, address=addr, values=raw_regs, unit_val=unit)
            else:
                return False, f"Function code {fc} not supported for write"

            if hasattr(res, "isError") and res.isError():
                return False, f"Modbus write error: {res}"
            if self.debug: 
                print(f"✅ wrote {value} (raw={raw_value}) -> {tag.name} (addr={addr}, fc={fc})")
            return True, None
        except Exception as e:
            return False, str(e)

    def _loop(self):
        if self.debug: print(f"🔄 RTU loop for {len(self.devices)} devices")
        schedule_next = {}
        while self.is_running:
            now = time.time()
            for dev in self.devices:
                interval = max(0.1, getattr(dev, "read_interval_ms", 1000) / 1000.0)
                if now >= schedule_next.get(dev.id, 0):
                    try:
                        self._read_device(dev)
                    except Exception as e:
                        print(f"❌ read device {getattr(dev,'name',dev.id)}: {e}")
                    schedule_next[dev.id] = now + interval
            time.sleep(0.01)

    def _read_device(self, device):
        tags = self.device_tags.get(device.id, [])
        if not tags: return
        if self.debug:
            print(f"📖 {device.name} (Unit {getattr(device,'unit_id',1)}) tags={len(tags)}")

        groups = {}
        for t in tags:
            fc = int(getattr(t, "function_code", 3) or 3)
            groups.setdefault(fc, []).append(t)

        all_rows = []
        for fc, tg_list in groups.items():
            values = self._read_block(device, fc, tg_list)
            if not values: continue

            for t in tg_list:
                raw = values.get(t.id)
                if raw is None: continue
                val = float(raw)
                sf = getattr(t, "scale_factor", 1.0) or 1.0
                off = getattr(t, "offset", 0.0) or 0.0
                if sf != 1.0: val *= sf
                if off != 0.0: val += off

                if self.db:
                    try:
                        self.db.update_tag_latest_value(t.id, val, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    except Exception as e:
                        if self.debug: print(f"⚠️ DB update tag {t.id} err: {e}")

                all_rows.append({
                    "id": t.id,
                    "name": t.name,
                    "value": val,
                    "datatype": getattr(t, "data_type", None) or getattr(t, "datatype", "Word"),
                    "unit": getattr(t, "unit", ""),
                    "ts": now_hms()
                })

        if all_rows:
            self._emit_modbus_update(device, all_rows)

    def _read_block(self, device, fc, tg_list):
        if not self.client: return {}
        tg_sorted = sorted(tg_list, key=lambda x: int(x.address))
        min_addr = int(tg_sorted[0].address)
        max_addr = int(tg_sorted[-1].address)

        max_regs = 1
        for t in tg_sorted:
            dtype = getattr(t, "data_type", None) or getattr(t, "datatype", "Word")
            if VC_AVAILABLE:
                max_regs = max(max_regs, get_register_count(dtype))
            else:
                dt = (dtype or "Word").lower()
                n = 4 if dt in ("double","float64") else (2 if dt in ("float","float32","real","int32","uint32","dint","dword","long") else 1)
                max_regs = max(max_regs, n)

        start = normalize_address(min_addr, self.address_base)
        end   = normalize_address(max_addr, self.address_base)
        count = (end - start + 1) + (max_regs - 1)
        unit  = getattr(device, "unit_id", 1)

        if self.debug:
            print(f"   🔍 FC{fc}: {min_addr}-{max_addr} -> start={start}, count={count}")

        try:
            if fc == 1:
                res = self._call_modbus(self.client.read_coils, address=start, count=count, unit_val=unit)
            elif fc == 2:
                res = self._call_modbus(self.client.read_discrete_inputs, address=start, count=count, unit_val=unit)
            elif fc == 3:
                res = self._call_modbus(self.client.read_holding_registers, address=start, count=count, unit_val=unit)
            elif fc == 4:
                res = self._call_modbus(self.client.read_input_registers, address=start, count=count, unit_val=unit)
            else:
                if self.debug: print(f"⚠️ unsupported FC {fc}")
                return {}
            if hasattr(res, "isError") and res.isError():
                if self.debug: print(f"❌ Modbus error FC{fc}: {res}")
                return {}
        except Exception as e:
            print(f"❌ Modbus read exception FC{fc}: {e}")
            return {}

        out = {}
        if fc in (1,2):
            bits = getattr(res, "bits", []) or []
            for t in tg_sorted:
                off = normalize_address(int(t.address), self.address_base) - start
                out[t.id] = 1 if (0 <= off < len(bits) and bits[off]) else 0
        else:
            regs = getattr(res, "registers", []) or []
            for t in tg_sorted:
                dtype = getattr(t, "data_type", None) or getattr(t, "datatype", "Word")
                byte_order = getattr(device, "byte_order", "BigEndian")
                word_order = getattr(t, "word_order", getattr(device, "word_order", "AB"))
                off = normalize_address(int(t.address), self.address_base) - start
                try:
                    if VC_AVAILABLE:
                        need = get_register_count(dtype)
                        chunk = regs[off:off+need]
                        val = convert_raw_value_to_web(chunk, dtype, byte_order, word_order)
                    else:
                        val = _fallback_unpack(regs, off, dtype, byte_order, word_order)
                    out[t.id] = val
                except Exception:
                    out[t.id] = None
        return out

    def _ensure_sio(self):
        if not SIO_AVAILABLE: return False
        
        # Check if already connected
        if self.sio and self.sio_connected:
            try:
                # Verify connection is still alive
                if self.sio.connected:
                    return True
                else:
                    # Connection is dead, mark as disconnected
                    self.sio_connected = False
            except:
                self.sio_connected = False
        
        # Try to connect/reconnect
        try:
            # If socket exists but not connected, disconnect first
            if self.sio and hasattr(self.sio, 'connected') and self.sio.connected:
                try:
                    self.sio.disconnect()
                except:
                    pass
            
            # Create new socket client if needed
            if not self.sio:
                self.sio = socketio.Client(reconnection=True)
            
            # Connect to server
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
            "latency_ms": 0,
            "tags": tag_rows,
            "ts": now_hms(),
        }
        if self._ensure_sio():
            try:
                self.sio.emit("modbus_update", payload)
                if self.debug:
                    print(f"   📡 Emitted {len(tag_rows)} tags for {device.name}")
            except Exception as e:
                print(f"⚠️ emit failed: {e}")
                self.sio_connected = False
        elif self.debug:
            print("⚠️  Socket.IO unavailable; payload not sent")
