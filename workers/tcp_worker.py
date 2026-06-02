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
import queue
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
# ---- centralized value formatter
try:
    from shared.formatting import format_tag_value, TagFormatMetadata as _TagFmtMeta
    _FORMATTER_AVAILABLE = True
except ImportError:
    _FORMATTER_AVAILABLE = False
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

# ---------- Decimal-places helper ----------
def _calc_decimal_places(scale):
    """Tính số chữ số thập phân từ giá trị scale.
    Ví dụ: scale=0.1 → 1, scale=0.01 → 2, scale=1 hoặc scale=10 → 0.
    Trả về None nếu không xác định được.
    """
    try:
        s = abs(float(scale))
        if s == 0.0:
            return 0
        scale_text = f"{s:.12f}".rstrip('0').rstrip('.')
        if '.' not in scale_text:
            return 0
        return min(6, len(scale_text.split('.', 1)[1]))
    except Exception:
        return None

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

        # Socket.IO (sync) - dùng reconnection=False để tự quản lý reconnection
        self.sio = None
        self.sio_connected = False
        self._sio_lock = threading.Lock()          # Lock bảo vệ thao tác SIO
        self._sio_last_ok = 0.0                    # Thời điểm emit thành công gần nhất
        self._sio_reconnect_after = 0.0            # Thời điểm cho phép reconnect tiếp
        self._sio_reconnect_backoff = 5            # Giây chờ giữa 2 lần reconnect
        self._sio_stale_sec = 60                   # Nếu > 60s không emit ok → coi là stale
        self._sio_consecutive_fails = 0            # Đếm số lần emit thất bại liên tiếp
        self._sio_max_fails_before_recreate = 5    # Sau N lần fail → tạo mới SIO client

        # Emission queue: device thread -> queue -> emission thread
        self._emit_queue = queue.Queue(maxsize=500)
        self._emit_thread = None

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

        # Socket.IO - khởi tạo kết nối ban đầu
        if SIO_AVAILABLE:
            self._create_sio_client()
            self._connect_sio()

        self.is_running = True

        # Khởi tạo emission thread (xử lý queue → emit Socket.IO)
        self._emit_thread = threading.Thread(target=self._emission_loop, daemon=True)
        self._emit_thread.start()

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

        # Dừng emission thread
        try:
            self._emit_queue.put_nowait(None)  # Poison pill
        except Exception:
            pass
        if self._emit_thread and self._emit_thread.is_alive():
            try:
                self._emit_thread.join(timeout=3)
            except Exception:
                pass

        # socket.io
        with self._sio_lock:
            try:
                if self.sio:
                    if getattr(self.sio, 'connected', False):
                        self.sio.disconnect()
                    self.sio = None
            except Exception:
                pass
            self.sio_connected = False

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
                        # ensure we don't keep a half-initialized client around
                        try:
                            cli.close()
                        except Exception:
                            pass
                        cli = None
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
                    elapsed_ms = int((time.perf_counter() - start_ts) * 1000)
                    if any_success:
                        consec_fail = 0
                        self._update_device_status(device, ok=True, latency_ms=elapsed_ms, err=None)
                    else:
                        consec_fail += 1
                        self._update_device_status(device, ok=False, latency_ms=elapsed_ms, err="No data read")
                        # If we repeatedly get no data (e.g., client not actually connected),
                        # trigger a reconnect like the exception path.
                        if consec_fail >= max_consec_fail:
                            try:
                                if cli:
                                    cli.close()
                            except Exception:
                                pass
                            cli = None
                            # short cool down to avoid tight loop
                            if stop_flag.wait(cool_down_sec):
                                break
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

                # Apply scale+offset → engineering units, round theo độ chính xác của scale
                val = float(raw)
                sf = getattr(t, "scale", 1.0) or 1.0
                off = getattr(t, "offset", 0.0) or 0.0
                if sf != 1.0: val *= sf
                if off != 0.0: val += off
                dp = _calc_decimal_places(sf)
                if dp is not None:
                    val = round(val, dp)

                # DB write latest - lưu giá trị đã qua scale+offset+round (engineering unit).
                # UI và datalogger chỉ cần hiển thị y nguyên, không cần transform thêm.
                try:
                    if update_tag_latest_value:
                        update_tag_latest_value(t.id, val, datetime.now())
                    elif self.db:
                        self.db.update_tag_latest_value(t.id, val, datetime.now())
                except Exception as e:
                    last_error = str(e)
                    if self.debug: print(f"⚠️ DB update tag {t.id} err: {e}")

                # Format string với đúng số chữ số thập phân để log và emit
                if _FORMATTER_AVAILABLE:
                    _dtype = getattr(t, "data_type", None) or getattr(t, "datatype", "Word")
                    _meta = _TagFmtMeta(t.id, sf, off, getattr(t, "unit", ""), _dtype)
                    val_str = format_tag_value(val, _meta).display_text
                elif dp is not None and dp > 0:
                    val_str = f"{val:.{dp}f}"
                elif dp == 0:
                    val_str = str(int(round(val)))
                else:
                    val_str = str(val)

                if self.debug:
                    print(f"   - {t.name} (addr={t.address}, fc={fc}) = {val_str} {getattr(t,'unit','')}")
                # Emit giá trị đã format đúng số thập phân; UI hiển thị trực tiếp
                all_rows.append({
                    "id": t.id,
                    "name": t.name,
                    "value": val_str,
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

        # Build normalized addresses first so mixed notations (e.g. 1 and 40001)
        # still generate a valid contiguous read range.
        tg_with_norm = []
        for t in tg_list:
            raw_addr = int(t.address)
            norm_addr = normalize_address(raw_addr, self.address_base)
            tg_with_norm.append((t, raw_addr, norm_addr))

        tg_with_norm.sort(key=lambda x: x[2])
        tg_sorted = [x[0] for x in tg_with_norm]

        min_addr = min(x[1] for x in tg_with_norm)
        max_addr = max(x[1] for x in tg_with_norm)

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

        start = min(x[2] for x in tg_with_norm)
        end   = max(x[2] for x in tg_with_norm)
        count = (end - start + 1) + (max_regs - 1)
        unit  = getattr(device, "unit_id", 1)

        if self.debug:
            print(f"   🔍 FC{fc}: raw={min_addr}-{max_addr} -> start={start}, count={count}")

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

    # ------------- Socket.IO client lifecycle (thread-safe) -------------
    def _create_sio_client(self):
        """Tạo mới hoàn toàn Socket.IO client. Gọi trong lock hoặc khi chưa multi-thread."""
        try:
            if self.sio:
                try:
                    self.sio.disconnect()
                except Exception:
                    pass
        except Exception:
            pass
        self.sio = socketio.Client(
            reconnection=False,           # Tự quản lý reconnection để tránh xung đột
            logger=False,
            engineio_logger=False,
        )
        self.sio_connected = False
        self._sio_consecutive_fails = 0
        self._setup_socketio_handlers()
        if self.debug:
            print("🔧 Created new Socket.IO client")

    def _connect_sio(self):
        """Kết nối Socket.IO. Trả True nếu thành công."""
        try:
            if self.sio and not getattr(self.sio, 'connected', False):
                self.sio.connect(self.webapp_url, wait=True, wait_timeout=10)
                self.sio_connected = True
                self._sio_last_ok = time.time()
                self._sio_consecutive_fails = 0
                if self.debug:
                    print("✅ Socket.IO connected")
                return True
            elif self.sio and self.sio.connected:
                self.sio_connected = True
                return True
        except Exception as e:
            print(f"⚠️  Socket.IO connect failed: {e}")
            self.sio_connected = False
        return False

    def _ensure_sio(self):
        """Thread-safe: kiểm tra & reconnect Socket.IO nếu cần."""
        if not SIO_AVAILABLE:
            return False

        # Fast path: kết nối còn sống và không bị stale
        if self.sio and self.sio_connected:
            try:
                if self.sio.connected:
                    # Kiểm tra stale: nếu quá lâu chưa emit ok → force reconnect
                    if self._sio_last_ok > 0 and (time.time() - self._sio_last_ok) > self._sio_stale_sec:
                        if self.debug:
                            print(f"⚠️  Socket.IO stale ({time.time() - self._sio_last_ok:.0f}s without success), forcing reconnect")
                    else:
                        return True
                else:
                    self.sio_connected = False
            except Exception:
                self.sio_connected = False

        # Kiểm tra cooldown: không reconnect quá nhanh
        now = time.time()
        if now < self._sio_reconnect_after:
            return False

        # Slow path: cần reconnect, dùng lock để chỉ 1 thread thực hiện
        acquired = self._sio_lock.acquire(blocking=False)
        if not acquired:
            # Thread khác đang reconnect, bỏ qua
            return self.sio_connected

        try:
            # Double-check sau khi lấy lock
            if self.sio and self.sio_connected and getattr(self.sio, 'connected', False):
                return True

            # Nếu fail quá nhiều → tạo client mới hoàn toàn
            if self._sio_consecutive_fails >= self._sio_max_fails_before_recreate:
                if self.debug:
                    print(f"🔄 Recreating Socket.IO client after {self._sio_consecutive_fails} consecutive failures")
                self._create_sio_client()
            else:
                # Đóng kết nối cũ nếu có
                try:
                    if self.sio and getattr(self.sio, 'connected', False):
                        self.sio.disconnect()
                except Exception:
                    pass
                self.sio_connected = False
                if not self.sio:
                    self._create_sio_client()

            ok = self._connect_sio()
            if not ok:
                self._sio_consecutive_fails += 1
                # Exponential backoff: 5s, 10s, 20s, 40s ... max 120s
                backoff = min(self._sio_reconnect_backoff * (2 ** min(self._sio_consecutive_fails - 1, 5)), 120)
                self._sio_reconnect_after = time.time() + backoff
                if self.debug:
                    print(f"⏳ Socket.IO reconnect failed (attempt {self._sio_consecutive_fails}), next retry in {backoff}s")
            return ok
        finally:
            self._sio_lock.release()

    # ------------- Emission queue (tách biệt emit khỏi device thread) -------------
    def _emission_loop(self):
        """Thread riêng xử lý emission queue → Socket.IO.
        Device threads chỉ đẩy payload vào queue, không bao giờ gọi emit trực tiếp."""
        while self.is_running:
            try:
                payload = self._emit_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            except Exception:
                continue

            if payload is None:  # Poison pill
                break

            if self._ensure_sio():
                try:
                    self.sio.emit("modbus_update", payload)
                    self._sio_last_ok = time.time()
                    self._sio_consecutive_fails = 0
                    if self.debug:
                        dev_name = payload.get('device_name', '?')
                        n_tags = len(payload.get('tags', []))
                        print(f"   📡 Emitted {n_tags} tags for {dev_name}")
                except Exception as e:
                    print(f"⚠️ emit failed: {e}")
                    self.sio_connected = False
                    self._sio_consecutive_fails += 1
            elif self.debug:
                # Drop payload khi không có kết nối — tránh queue tràn
                pass

    def _emit_modbus_update(self, device, tag_rows):
        """Đẩy payload vào emission queue (non-blocking). Không emit trực tiếp."""
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
        try:
            self._emit_queue.put_nowait(payload)
        except queue.Full:
            # Queue đầy → drop payload cũ nhất, đẩy cái mới vào
            try:
                self._emit_queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._emit_queue.put_nowait(payload)
            except Exception:
                pass
