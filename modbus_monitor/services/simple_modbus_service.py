"""
Simplified ModbusService với direct socket emission để debug UI update issue
"""
from __future__ import annotations
import threading, time, math, os
from queue import Queue
from typing import List, Dict, Tuple, Optional
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

class SimpleDeviceReader:
    """Simplified device reader với direct socket emission"""
    
    def __init__(self, dev_row: Dict, db_queue: Queue, push_queue: Queue, cache: LatestCache):
        self.d = dev_row
        self.dbq = db_queue
        self.pushq = push_queue
        self.cache = cache
        self.client = None
        self.byte_order = self.d.get("byte_order") or "BigEndian"
        self.word_order = self.d.get("word_order") or "AB"
        self.unit_id = int(self.d.get("unit_id") or 1)
        self.timeout = min((int(self.d.get("timeout_ms") or 200))/1000.0, 0.2)
        self._connected = False
        self._backoff = 1.0
        self._next_retry_ts = 0.0
        self._seq = 0
        self._device_id_str = f"dev{self.d['id']}"

    def _connect(self) -> bool:
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
                print(f"🔌 Connecting to ModbusRTU: port={serial_port}, baudrate={baudrate}")
                self.client = ModbusSerialClient(
                    port=serial_port,
                    baudrate=baudrate,
                    bytesize=int(self.d.get("bytesize") or 8),
                    parity=parity,
                    stopbits=int(self.d.get("stopbits") or 1),
                    timeout=self.timeout
                )
            
            connected = self.client.connect()
            return connected
            
        except Exception as e:
            print(f"❌ Connection error for {self.d.get('name')}: {e}")
            return False

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
            
            # Direct socket emission for connection success
            try:
                from modbus_monitor.extensions import socketio
                socketio.emit("modbus_update", {
                    "device_id": self._device_id_str,
                    "device_name": self.d.get("name", "Unknown"),
                    "unit": self.d.get("unit_id", 1),
                    "ok": True,
                    "status": "connected",
                    "seq": self._seq,
                    "ts": datetime.now().strftime("%H:%M:%S")
                }, room=f"dashboard_device_{self.d['id']}")
            except Exception as e:
                print(f"Socket emission error: {e}")
        else:
            retry_delay = min(self._backoff, 30.0)
            self._backoff = min(self._backoff * 1.5, 30.0)
            self._next_retry_ts = now + retry_delay
        
        return ok

    def _close(self):
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass
        self.client = None
        self._connected = False

    def _read_registers(self, address: int, count: int, function_code: int = 3):
        """Simple register read"""
        if count <= 0:
            return [None] * count

        try:
            if function_code == 3:  # Holding Registers
                rr = self.client.read_holding_registers(address, count=count, slave=self.unit_id)
                if rr.isError():
                    return [None] * count
                return rr.registers
            # Add other function codes as needed
            
        except Exception as e:
            self._connected = False
            self._close()
            return [None] * count

    def _extract_simple(self, regs: list[int], offset: int, datatype: str, scale: float, offs: float):
        """Simplified extraction for common datatypes"""
        if offset >= len(regs) or regs[offset] is None:
            return None
            
        try:
            name = (datatype or "").strip().lower()
            
            if name in ("unsigned", "word", "uint16", "ushort"):
                val = regs[offset]
            elif name in ("signed", "short", "int16"):
                val = regs[offset]
                if val > 32767:
                    val = val - 65536
            elif name in ("float", "float32", "real"):
                if offset + 1 >= len(regs):
                    return None
                lo, hi = regs[offset], regs[offset+1]
                w1, w2 = (hi, lo) if self.word_order == "AB" else (lo, hi)
                b = w1.to_bytes(2, "big") + w2.to_bytes(2, "big")
                val = struct.unpack(">f", b)[0]
            else:
                val = regs[offset]  # Default to unsigned
            
            return val * scale + offs
            
        except Exception:
            return None

    def loop_once(self):
        """Simplified loop với direct socket emission"""
        t0 = time.perf_counter()
        
        if not self._ensure_connected():
            # Direct socket emission for disconnection
            try:
                from modbus_monitor.extensions import socketio
                socketio.emit("modbus_update", {
                    "device_id": self._device_id_str,
                    "device_name": self.d.get("name", "Unknown"),
                    "unit": self.d.get("unit_id", 1),
                    "ok": False,
                    "error": "Connection failed",
                    "status": "disconnected",
                    "seq": self._seq,
                    "ts": datetime.now().strftime("%H:%M:%S")
                }, room=f"dashboard_device_{self.d['id']}")
            except Exception:
                pass
            return

        # Get tags from DB (simplified, no caching)
        tags = dbsync.list_tags(self.d["id"])
        if not tags:
            return

        ts = utc_now()
        self._seq += 1
        all_successful_tags = []

        # Group tags by address range for bulk read
        addresses = [int(t["address"]) for t in tags]
        if addresses:
            min_addr = min(addresses)
            max_addr = max(addresses)
            count = max_addr - min_addr + 1
            
            # Read bulk data
            bulk_data = self._read_registers(min_addr, count)
            
            if bulk_data and not all(r is None for r in bulk_data):
                # Process each tag
                for t in tags:
                    try:
                        addr = int(t["address"])
                        offset = addr - min_addr
                        scale = float(t.get("scale") or 1.0)
                        offs = float(t.get("offset") or 0.0)
                        
                        val = self._extract_simple(bulk_data, offset, t["datatype"], scale, offs)
                        
                        if val is not None:
                            # Cache and queue
                            self.cache.set(int(t["id"]), ts, val)
                            self.dbq.put((int(t["id"]), ts, float(val)))
                            
                            # Add to emission list
                            all_successful_tags.append({
                                "id": int(t["id"]),
                                "name": t.get("name", "tag_test"),
                                "value": float(val),
                                "datatype": t["datatype"],
                                "ts": datetime.now().strftime("%H:%M:%S")
                            })
                    except Exception as e:
                        print(f"Error processing tag {t.get('name', '?')}: {e}")

        # Direct socket emission
        if all_successful_tags:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            
            try:
                from modbus_monitor.extensions import socketio
                
                # Emit to main dashboard
                socketio.emit("modbus_update", {
                    "device_id": self._device_id_str,
                    "device_name": self.d.get("name", "Unknown"),
                    "unit": self.d.get("unit_id", 1),
                    "ok": True,
                    "tags": all_successful_tags,
                    "seq": self._seq,
                    "latency_ms": latency_ms,
                    "ts": datetime.now().strftime("%H:%M:%S")
                }, room=f"dashboard_device_{self.d['id']}")
                
                print(f"✅ Emitted {len(all_successful_tags)} tags for device {self.d.get('name')}")
                
            except Exception as e:
                print(f"❌ Socket emission failed: {e}")

    def loop_with_timing(self, start_epoch: float, barrier: threading.Barrier):
        """Simple timing loop"""
        try:
            barrier.wait(timeout=10.0)
        except:
            pass
        
        # Wait until start time
        now = time.monotonic()
        if now < start_epoch:
            time.sleep(start_epoch - now)
        
        interval = 1.0  # 1 second interval
        next_run = start_epoch
        
        while True:
            now = time.monotonic()
            if now < next_run:
                time.sleep(next_run - now)
            
            try:
                self.loop_once()
            except Exception as e:
                print(f"Error in loop: {e}")
            
            next_run += interval
            
            # Anti-drift
            while time.monotonic() >= next_run:
                next_run += interval

class SimpleModbusService:
    """Simplified Modbus service để debug UI issues"""
    
    def __init__(self, db_queue: Queue, push_queue: Queue, cache: LatestCache):
        self.dbq = db_queue
        self.pushq = push_queue
        self.cache = cache
        self._threads: Dict[int, threading.Thread] = {}
        self._readers: Dict[int, SimpleDeviceReader] = {}

    def start(self):
        devices = dbsync.list_devices()
        if not devices:
            print("No devices found")
            return

        barrier = threading.Barrier(len(devices))
        start_epoch = math.ceil(time.monotonic()) + 1
        
        for d in devices:
            try:
                reader = SimpleDeviceReader(d, self.dbq, self.pushq, self.cache)
                self._readers[d["id"]] = reader
                
                t = threading.Thread(
                    target=reader.loop_with_timing,
                    args=(start_epoch, barrier),
                    daemon=True,
                    name=f"Simple-{d['name']}"
                )
                
                t.start()
                self._threads[d["id"]] = t
                print(f"✅ Started simple reader for device: {d['name']}")
                
            except Exception as e:
                print(f"❌ Failed to start device {d.get('name')}: {e}")

    def stop(self):
        print("Stopping simple modbus service...")
        for t in self._threads.values():
            t.join(timeout=2)
        self._readers.clear()
        print("Simple modbus service stopped")

    def write_tag_value(self, tag_id: int, value: float) -> bool:
        # Simplified write - can be implemented later
        print(f"Write not implemented in simple service: tag {tag_id} = {value}")
        return False

# Replace the complex ModbusService with simple version for debugging
#ModbusService = SimpleModbusService
