#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TCP Worker - Modbus TCP (pymodbus v2/v3 compatible: unit/slave auto-detect)
- Mỗi device (unit_id) chạy độc lập trên 1 thread + 1 TCP client riêng
- Block-read theo function code, map offset từng tag
- Hỗ trợ write (coil, holding register 1/2/4 regs) qua Socket.IO
- Dùng converter riêng: convert_raw_value_to_web / convert_web_value_to_raw / get_register_count
- Circuit-breaker: sau N lỗi liên tiếp, nghỉ X giây rồi reconnect, tránh spam gateway
"""

import os
import sys
import time
import struct
import inspect
import threading
from datetime import datetime

# ---- sys.path để tìm utils/shared
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))

# (Tuỳ bạn có dùng trực tiếp update_* từ DB module)
try:
    from webapp.modbus_monitor.database.db import update_device_row, update_tag_latest_value
except Exception:
    # fallback an toàn nếu import trực tiếp không có
    update_device_row = None
    update_tag_latest_value = None

# ---- Optional: DB wrapper (nếu bạn có dùng DatabaseManager)
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
    from pymodbus.client import ModbusTcpClient
    PYMODBUS_AVAILABLE = True
except Exception:
    PYMODBUS_AVAILABLE = False
    ModbusTcpClient = None
    print("⚠️  pymodbus not available. Worker will run in no-PLC mode.")

# ---- value converter
try:
    try:
        from utils.value_converter import (
            convert_raw_value_to_web,
            convert_web_value_to_raw,
            get_register_count,
        )
    except ImportError:
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

def _fallback_unpack(regs, offset, datatype="Word", byte_order="BigEndian", word_order="AB"):
    dt = (datatype or "Word").lower()
    if dt in ("word", "uint16", "unsigned", "ushort"): return regs[offset]
    if dt in ("signed", "int16", "short"):
        v = regs[offset]
        return v if v < 32768 else v - 65536
    if dt in ("float", "float32", "real", "ieee754"):
        if offset + 1 >= len(regs): return None
        hi, lo = (regs[offset], regs[offset + 1]) if word_order.upper() == "AB" else (regs[offset+1], regs[offset])
        b = hi.to_bytes(2, "big") + lo.to_bytes(2, "big")
        if byte_order == "LittleEndian":
            b = b[1:2]+b[0:1]+b[3:4]+b[2:3]
        return struct.unpack(">f", b)[0]
    if dt in ("int32", "dint", "long"):
        if offset + 1 >= len(regs): return None
        hi, lo = (regs[offset], regs[offset + 1]) if word_order.upper() == "AB" else (regs[offset+1], regs[offset])
        val = (hi << 16) | lo
        return val if val < 2147483648 else val - 4294967296
    if dt in ("uint32", "dword"):
        if offset + 1 >= len(regs): return None
        hi, lo = (regs[offset], regs[offset + 1]) if word_order.upper() == "AB" else (regs[offset+1], regs[offset])
        return (hi << 16) | lo
    return regs[offset]

# ---------- Worker ----------
class TCPWorker:
    def __init__(self, worker_id, host, port=502, timeout=2.0,
                 devices=None, tags=None, webapp_url="http://127.0.0.1:5000",
                 address_base="auto", debug=True):
        """
        devices: list obj có .id, .name, .unit_id, .read_interval_ms, ...
        tags: list obj có .id, .device_id, .name, .address, .function_code, .data_type/.datatype, .scale, .offset, .unit, ...
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

        # Nhóm tag theo device
        self.device_tags = {}
        for tg in self.tags:
            self.device_tags.setdefault(tg.device_id, []).append(tg)

        # DB wrapper (tuỳ có dùng hay không)
        self.db = DatabaseManager() if DB_AVAILABLE else None

        # Socket.IO (sync)
        self.sio = socketio.Client(reconnection=True) if SIO_AVAILABLE else None
        self.sio_connected = False

        # MỖI DEVICE 1 CLIENT & 1 THREAD
        self.clients = {}            # { device.id: ModbusTcpClient }
        self.device_threads = {}     # { device.id: Thread }
        self.device_stop_flags = {}  # { device.id: Event }

        # Pymodbus unit/slave keyword
        self._unit_kw = "unit"       # auto-detect theo client đầu tiên khi gọi

        # lifecycle
        self.is_running = False

    # ---------------- Lifecycle ----------------
    def start(self):
        if self.is_running:
            if self.debug: print("⚠️  TCPWorker already running")
            return True

        # Socket.IO
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

        # Tạo thread riêng cho từng device
        for dev in self.devices:
            stop_flag = threading.Event()
            self.device_stop_flags[dev.id] = stop_flag
            t = threading.Thread(target=self._device_loop, args=(dev, stop_flag), daemon=True)
            self.device_threads[dev.id] = t
            t.start()
            # Stagger nhẹ để tránh đồng thời
            time.sleep(0.2)

        return True

    def stop(self):
        self.is_running = False

        # dừng từng device loop
        for dev_id, ev in list(self.device_stop_flags.items()):
            ev.set()
        for dev_id, th in list(self.device_threads.items()):
            try:
                th.join(timeout=3)
            except Exception:
                pass
        self.device_threads.clear()
        self.device_stop_flags.clear()

        # đóng clients riêng
        for dev_id, cli in list(self.clients.items()):
            try:
                cli.close()
            except Exception:
                pass
        self.clients.clear()

        # socket.io
        try:
            if self.sio and self.sio_connected:
                self.sio.disconnect()
        except Exception:
            pass

        for dev in self.devices:
            self._update_device_status(dev, ok=False, latency_ms=None, err="worker stopped")
        print("🛑 TCPWorker stopped")

    # ---------------- Per-device loop + circuit breaker ----------------
    def _device_loop(self, device, stop_flag: threading.Event):
        """Polling riêng từng device; không block nhau."""
        if not PYMODBUS_AVAILABLE:
            self._update_device_status(device, ok=False, err="pymodbus not available")
            return

        interval = max(0.1, getattr(device, "read_interval_ms", 1000) / 1000.0)
        cool_down_sec = 5          # nghỉ khi lỗi liên tiếp
        max_consec_fail = 3
        consec_fail = 0

        cli = None

        while self.is_running and not stop_flag.is_set():
            # đảm bảo client sống cho device này
            if cli is None:
                try:
                    cli = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout)
                    if not cli.connect():
                        raise RuntimeError("TCP connect failed")
                    self.clients[device.id] = cli
                    # autodetect unit/slave keyword 1 lần
                    self._unit_kw = self._detect_unit_keyword(cli)
                    if self.debug:
                        print(f"✅ [{device.name}] TCP connected ({self.host}:{self.port}), kw='{self._unit_kw}'")
                    consec_fail = 0
                except Exception as e:
                    self._update_device_status(device, ok=False, latency_ms=None, err=str(e))
                    consec_fail += 1
                    # nghỉ chút rồi thử lại
                    if stop_flag.wait(min(cool_down_sec, interval)):
                        break
                    continue

            start_ts = time.perf_counter()
            try:
                    # Modified: _read_device_with_client returns True if any tag read succeeded
                    any_success = self._read_device_with_client(device, cli)
                    print(any_success)
                    elapsed_ms = int((time.perf_counter() - start_ts) * 1000)
                    if any_success:
                        consec_fail = 0
                        self._update_device_status(device, ok=True, latency_ms=elapsed_ms, err=None)
                    else:
                        consec_fail += 1
                        self._update_device_status(device, ok=False, latency_ms=elapsed_ms, err="No data read")
            except Exception as e:
                consec_fail += 1
                elapsed_ms = int((time.perf_counter() - start_ts) * 1000)
                self._update_device_status(device, ok=False, latency_ms=elapsed_ms, err=str(e))

                # nếu lỗi liên tiếp nhiều lần → reset client + cooldown
                if consec_fail >= max_consec_fail:
                    try:
                        cli.close()
                    except Exception:
                        pass
                    cli = None
                    if stop_flag.wait(cool_down_sec):
                        break

            # chờ đến chu kỳ tiếp theo
            if stop_flag.wait(interval):
                break

        # đóng client khi thoát loop
        try:
            if cli:
                cli.close()
        except Exception:
            pass
        self.clients.pop(device.id, None)

    # ---------------- Device read using per-device client ----------------
    def _read_device_with_client(self, device, client):
        tags = self.device_tags.get(device.id, [])
        if not tags:
            return False

        if self.debug:
            print(f"📖 {device.name} (Unit {getattr(device,'unit_id',1)}) tags={len(tags)}")

        # group theo FC
        groups = {}
        for t in tags:
            fc = int(getattr(t, "function_code", 3) or 3)
            groups.setdefault(fc, []).append(t)

        all_rows = []
        any_success = False
        last_error = None

        for fc, tg_list in groups.items():
            try:
                values = self._read_block_with_client(client, device, fc, tg_list)
                if not values:
                    continue
            except Exception as e:
                last_error = str(e)
                continue

            for t in tg_list:
                raw = values.get(t.id)
                if raw is None:
                    continue

                any_success = True

                # scale/offset
                val = float(raw)
                sf = getattr(t, "scale", 1.0) or 1.0
                off = getattr(t, "offset", 0.0) or 0.0
                if sf != 1.0: val *= sf
                if off != 0.0: val += off

                # DB write latest
                try:
                    if update_tag_latest_value:
                        update_tag_latest_value(t.id, val, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    elif self.db:
                        self.db.update_tag_latest_value(t.id, val, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                except Exception as e:
                    last_error = str(e)
                    if self.debug: print(f"⚠️ DB update tag {t.id} err: {e}")

                # format hiển thị
                if sf != 1.0:
                    formatted_value = round(val, 1) if val == int(val) else round(val, 2)
                else:
                    formatted_value = round(val, 2)
                if sf == 1.0 and isinstance(val, float) and val.is_integer():
                    formatted_value = int(val)

                if self.debug:
                    print(f"   - {t.name} (addr={t.address}, fc={fc}) = {formatted_value} {getattr(t,'unit','')}")
                all_rows.append({
                    "id": t.id,
                    "name": t.name,
                    "value": str(formatted_value),
                    "datatype": getattr(t, "data_type", None) or getattr(t, "datatype", "Word"),
                    "unit": getattr(t, "unit", ""),
                    "ts": now_hms()
                })

        if all_rows:
            self._emit_modbus_update(device, all_rows)
            return True

        if not any_success and last_error:
            raise RuntimeError(last_error)

    # ---------------- Block read (per-device client) ----------------
    def _read_block_with_client(self, client, device, fc, tg_list):
        if not client:
            return {}

        tg_sorted = sorted(tg_list, key=lambda x: int(x.address))
        min_addr = int(tg_sorted[0].address)
        max_addr = int(tg_sorted[-1].address)

        # số regs tối đa cần cho datatype dài nhất
        max_regs = 1
        for t in tg_sorted:
            dtype = getattr(t, "data_type", None) or getattr(t, "datatype", "Word")
            if VC_AVAILABLE:
                max_regs = max(max_regs, get_register_count(dtype))
            else:
                dt = (dtype or "Word").lower()
                need = 4 if dt in ("double","float64") else (2 if dt in ("float","float32","real","int32","uint32","dint","dword","long") else 1)
                max_regs = max(max_regs, need)

        start = normalize_address(min_addr, self.address_base)
        end   = normalize_address(max_addr, self.address_base)
        count = (end - start + 1) + (max_regs - 1)
        unit  = getattr(device, "unit_id", 1)

        if self.debug:
            print(f"   🔍 FC{fc}: {min_addr}-{max_addr} -> start={start}, count={count}")

        try:
            if fc == 1:
                res = self._call_modbus(client.read_coils, address=start, count=count, unit_val=unit)
            elif fc == 2:
                res = self._call_modbus(client.read_discrete_inputs, address=start, count=count, unit_val=unit)
            elif fc == 3:
                res = self._call_modbus(client.read_holding_registers, address=start, count=count, unit_val=unit)
            elif fc == 4:
                res = self._call_modbus(client.read_input_registers, address=start, count=count, unit_val=unit)
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
        if fc in (1, 2):
            bits = getattr(res, "bits", []) or []
            for t in tg_sorted:
                off = normalize_address(int(t.address), self.address_base) - start
                out[t.id] = 1 if (0 <= off < len(bits) and bits[off]) else 0
        else:
            regs = getattr(res, "registers", []) or []
            for t in tg_sorted:
                dtype = getattr(t, "data_type", None) or getattr(t, "datatype", "Word")
                byte_order = "BigEndian"
                dl = (dtype or "Word").lower()
                if dl in ("signed","unsigned","hex","binary","word"): word_order = "AB"
                elif dl in ("float","float32","real"):               word_order = "BA"
                elif dl == "float_inverse":                           word_order = "AB"
                elif dl in ("double","float64"):                      word_order = "BADC"
                elif dl == "double_inverse":                          word_order = "ABDC"
                elif dl in ("long","int32","uint32","dint","dword"):  word_order = "BA"
                elif dl == "long_inverse":                            word_order = "AB"
                else:                                                 word_order = "AB"
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

    # ---------------- Write tag ----------------
    def _write_tag(self, device, tag, value):
        """Write sync — ưu tiên dùng client sẵn có của device; nếu không có thì mở tạm."""
        if not PYMODBUS_AVAILABLE:
            if self.debug: print(f"⚠️ simulate write {value} -> {tag.name}")
            return True, None

        client = self.clients.get(device.id)
        temp_client = None
        try:
            if client is None:
                temp_client = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout)
                if not temp_client.connect():
                    return False, "TCP connect failed for write"
                client = temp_client
                # detect unit/slave nếu chưa có
                self._unit_kw = self._detect_unit_keyword(client)

            unit = getattr(device, "unit_id", 1)
            addr = normalize_address(int(tag.address), self.address_base)
            fc   = int(getattr(tag, "function_code", 3) or 3)
            dtype = getattr(tag, "data_type", None) or getattr(tag, "datatype", "Word")

            # byte/word order theo datatype
            byte_order = "BigEndian"
            dl = (dtype or "Word").lower()
            if dl in ("signed","unsigned","hex","binary","word"): word_order = "AB"
            elif dl in ("float","float32","real"):               word_order = "BA"
            elif dl == "float_inverse":                           word_order = "AB"
            elif dl in ("double","float64"):                      word_order = "BADC"
            elif dl == "double_inverse":                          word_order = "ABDC"
            elif dl in ("long","int32","uint32","dint","dword"):  word_order = "BA"
            elif dl == "long_inverse":                            word_order = "AB"
            else:                                                 word_order = "AB"

            scale  = getattr(tag, "scale", 1.0) or 1.0
            offset = getattr(tag, "offset", 0.0) or 0.0

            raw_value = float(value)
            if scale != 1.0:
                raw_value /= scale
            raw_value = round(raw_value, 2)
            if self.debug:
                print(f"📝 Write conversion: {value} -> {raw_value} (offset={offset}, scale={scale})")

            if VC_AVAILABLE:
                raw_regs = convert_web_value_to_raw(raw_value, dtype, byte_order, word_order)
            else:
                raw_regs = [int(raw_value)]

            if fc == 1:
                res = self._call_modbus(client.write_coil, address=addr, value=bool(raw_regs[0]), unit_val=unit)
            elif fc == 3:
                if len(raw_regs) == 1:
                    res = self._call_modbus(client.write_register, address=addr, value=raw_regs[0], unit_val=unit)
                else:
                    res = self._call_modbus(client.write_registers, address=addr, values=raw_regs, unit_val=unit)
            else:
                return False, f"Function code {fc} not supported for write"

            if hasattr(res, "isError") and res.isError():
                return False, f"Modbus write error: {res}"
            if self.debug:
                print(f"✅ wrote {value} (raw={raw_value}) -> {tag.name} (addr={addr}, fc={fc})")
            return True, None
        except Exception as e:
            return False, str(e)
        finally:
            if temp_client:
                try: temp_client.close()
                except Exception: pass

    # ---------------- Socket.IO (write command) ----------------
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
            if not data or not isinstance(data, dict):
                print(f"❌ Invalid write command data: {data}")
                return
            try:
                tag_id = data.get('tag_id')
                value  = data.get('value')
                if tag_id is None or value is None:
                    self._send_write_response(tag_id, False, "Missing tag_id/value", data.get('frontend_client_id'))
                    return

                tag = next((t for t in self.tags if t.id == tag_id), None)
                if not tag:
                    if self.debug: print(f"⏭️  Tag {tag_id} không thuộc worker này")
                    return

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

    # ---------------- Modbus call helpers ----------------
    def _detect_unit_keyword(self, client):
        try:
            sig = inspect.signature(client.read_holding_registers)
            params = sig.parameters
            if "unit" in params: return "unit"
            if "slave" in params: return "slave"
        except Exception:
            pass
        return "unit"

    def _call_modbus(self, func, /, unit_val=1, **kwargs):
        """
        Gọi pymodbus (read/write) với kw 'unit' hoặc 'slave'.
        Ưu tiên self._unit_kw; nếu TypeError -> thử kw còn lại và ghi nhớ.
        """
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

    # ---------------- Device status + Socket.IO emit ----------------
    def _update_device_status(self, device, *, ok: bool, latency_ms=None, err=None):
        data = {
            "is_online": ok,
            "updated_at": datetime.now(),
        }
        try:
            if update_device_row:
                update_device_row(device.id, data)
            elif self.db:
                self.db.update_device_row(device.id, data)
            if self.debug:
                print(f"🔄 Device {getattr(device,'name',device.id)} status: {'OK' if ok else 'FAIL'} "
                      f"(latency={latency_ms}ms, err={err})")
        except Exception as e:
            if self.debug:
                print(f"⚠️ DB update device {getattr(device,'name',device.id)} err: {e}")

    def _ensure_sio(self):
        if not SIO_AVAILABLE:
            return False

        if self.sio and self.sio_connected:
            try:
                if self.sio.connected:
                    return True
                else:
                    self.sio_connected = False
            except Exception:
                self.sio_connected = False

        try:
            if self.sio and getattr(self.sio, 'connected', False):
                try:
                    self.sio.disconnect()
                except Exception:
                    pass
            if not self.sio:
                self.sio = socketio.Client(reconnection=True)
                self._setup_socketio_handlers()
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
