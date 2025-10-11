#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
orchestra_modbus.py
- Đọc cấu hình worker từ DB (webapp.modbus_monitor.database.db) và khởi tạo các Modbus workers (TCP/RTU)
- Hỗ trợ hot-reload đơn giản: định kỳ đọc DB để START các worker mới bật auto_start
- Dừng gọn khi Ctrl+C / SIGTERM

Chạy:
    python orchestra_modbus.py --webapp-url http://127.0.0.1:5000 --refresh-sec 15
"""

import argparse
import os
import sys
import signal
import time
from types import SimpleNamespace
from threading import Event
# ---------------- Import worker classes (nằm trong thư mục workers) ----------------
TCPWorker = None
RTUWorker = None

# Ưu tiên bản đặt tên "modbus_*_worker"
try:
    from workers.modbus_tcp_worker import ModbusTCPWorker as TCPWorker
    print("✅ Loaded TCP worker from workers.modbus_tcp_worker.ModbusTCPWorker")
except Exception as e:
    try:
        from workers.tcp_worker import TCPWorker as TCPWorker
        print("✅ Loaded TCP worker from workers.tcp_worker.TCPWorker")
    except Exception as e2:
        print(f"⚠️ Không tải được TCP worker: {e} / {e2}")

try:
    from workers.modbus_rtu_worker import ModbusRTUWorker as RTUWorker
    print("✅ Loaded RTU worker from workers.modbus_rtu_worker.ModbusRTUWorker")
except Exception as e:
    try:
        from workers.rtu_worker import RTUWorker as RTUWorker
        print("✅ Loaded RTU worker from workers.rtu_worker.RTUWorker")
    except Exception as e2:
        print(f"⚠️ Không tải được RTU worker: {e} / {e2}")

# ---------------- Import DB module at webapp/modbus_monitor/database/db.py ----------------
# Thêm thư mục webapp vào sys.path để import được modbus_monitor
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
WEBAPP_DIR = os.path.join(THIS_DIR, "webapp")
if WEBAPP_DIR not in sys.path:
    sys.path.insert(0, WEBAPP_DIR)

try:
    from modbus_monitor.database import db as db
    print("✅ Successfully imported DB module")
except Exception as e:
    print(f"❌ Không import được DB module modbus_monitor.database.db: {e}")
    raise

# ---------------- Import worker classes (nằm cạnh orchestra) ----------------
try:
    from workers.tcp_worker import TCPWorker
except Exception as e:
    print(f"⚠️ Không tải được TCPWorker: {e}")
    TCPWorker = None

try:
    from workers.rtu_worker import RTUWorker
except Exception as e:
    print(f"⚠️ Không tải được RTUWorker: {e}")
    RTUWorker = None


# ---------------- Helpers: chuyển dict -> object có field worker đang dùng ----------------
def _ns_device(d: dict) -> SimpleNamespace:
    return SimpleNamespace(
        id=d.get("id"),
        name=d.get("name", f"Device {d.get('id')}"),
        protocol=d.get("protocol", "ModbusTCP"),
        host=d.get("host"),
        port=d.get("port", 502),
        serial_port=d.get("serial_port"),
        baudrate=d.get("baudrate", 9600),
        bytesize=d.get("bytesize", 8),
        parity=d.get("parity", "N"),
        stopbits=d.get("stopbits", 1),
        unit_id=d.get("unit_id", 1),
        timeout_ms=d.get("timeout_ms", 2000),
        read_interval_ms=d.get("read_interval_ms", 1000),
        default_function_code=d.get("default_function_code", 3),
        byte_order=d.get("byte_order", "BigEndian"),
        word_order=d.get("word_order", "AB"),
        description=d.get("description", "")
    )


def _ns_tag(t: dict) -> SimpleNamespace:
    return SimpleNamespace(
        id=t.get("id"),
        device_id=t.get("device_id"),
        name=t.get("name", f"Tag {t.get('id')}"),
        address=int(t.get("address", 0) or 0),
        function_code=t.get("function_code") if t.get("function_code") is not None else None,
        data_type=t.get("datatype", "Word"),
        datatype=t.get("datatype", "Word"),
        scale_factor=float(t.get("scale", 1.0) or 1.0),
        scale=float(t.get("scale", 1.0) or 1.0),
        offset=float(t.get("offset", 0.0) or 0.0),
        description=t.get("description", ""),
        unit=t.get("unit"),
    )


# ---------------- Orchestrator ----------------
class ModbusOrchestrator:
    def __init__(self, webapp_url: str):
        self.webapp_url = webapp_url
        self.stop_event = Event()
        self.workers = {}  # worker_id -> instance

    def _start_tcp_worker(self, cfg: dict):
        if TCPWorker is None:
            print("❌ TCPWorker chưa khả dụng, bỏ qua worker TCP")
            return

        devices_ns = [_ns_device(d) for d in cfg.get("devices", [])]
        tags_ns = [_ns_tag(t) for t in cfg.get("tags", [])]

        # Host/port ưu tiên từ cấu hình worker; nếu thiếu thì fallback sang device đầu tiên
        host = cfg.get("host")
        port = cfg.get("port")
        if (not host or not port) and devices_ns:
            host = host or devices_ns[0].host
            port = port or devices_ns[0].port

        worker = TCPWorker(
            worker_id=cfg["worker_id"],
            host=host,
            port=port or 502,
            timeout=5,
            devices=devices_ns,
            tags=tags_ns,
            webapp_url=self.webapp_url,
        )
        ok = worker.start()
        if ok:
            self.workers[cfg["worker_id"]] = worker
            print(f"🚀 TCP worker '{cfg['worker_id']}' started ({host}:{port})")
        else:
            print(f"❌ Không khởi động được TCP worker '{cfg['worker_id']}'")

    def _start_rtu_worker(self, cfg: dict):
        if RTUWorker is None:
            print("❌ RTUWorker chưa khả dụng, bỏ qua worker RTU")
            return

        devices_ns = [_ns_device(d) for d in cfg.get("devices", [])]
        tags_ns = [_ns_tag(t) for t in cfg.get("tags", [])]

        serial_port = cfg.get("serial_port")
        baudrate = int(cfg.get("baudrate") or (devices_ns[0].baudrate if devices_ns else 9600))

        worker = RTUWorker(
            worker_id=cfg["worker_id"],
            serial_port=serial_port,
            baudrate=baudrate,
            timeout=5,
            devices=devices_ns,
            tags=tags_ns,
        )
        ok = worker.start()
        if ok:
            self.workers[cfg["worker_id"]] = worker
            print(f"🚀 RTU worker '{cfg['worker_id']}' started ({serial_port} @ {baudrate})")
        else:
            print(f"❌ Không khởi động được RTU worker '{cfg['worker_id']}'")

    def start_autostart_workers(self):
        print("🔎 Đang tải cấu hình worker auto-start từ DB ...")
        # Hàm này bạn đã implement trong webapp/modbus_monitor/database/db.py
        worker_rows = db.get_auto_start_workers()
        print(f"📋 Tìm thấy {len(worker_rows)} worker auto-start")

        for cfg in worker_rows:
            if not cfg.get("enabled", True) or not cfg.get("auto_start", True):
                continue
            wid = cfg["worker_id"]
            if wid in self.workers:
                # đã chạy
                continue
            wtype = (cfg.get("worker_type") or "").lower()
            if wtype == "tcp":
                self._start_tcp_worker(cfg)
            elif wtype == "rtu":
                self._start_rtu_worker(cfg)
            else:
                print(f"⚠️ Worker '{wid}' có worker_type không hỗ trợ: {cfg.get('worker_type')}")

    def stop_all(self):
        print("🛑 Dừng tất cả workers ...")
        for wid, w in list(self.workers.items()):
            try:
                w.stop()
            except Exception as e:
                print(f"⚠️ Lỗi dừng worker {wid}: {e}")
        self.workers.clear()

    def run(self, refresh_sec: int = 15):
        def _sig_handler(signum, frame):
            print(f"\n🚦 Nhận tín hiệu {signum}, đang dừng ...")
            self.stop_event.set()

        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)

        # Start lần đầu
        self.start_autostart_workers()
        print(f"🔁 Hot-reload {'ON' if refresh_sec > 0 else 'OFF'} (chu kỳ {refresh_sec}s)")

        try:
            while not self.stop_event.is_set():
                if refresh_sec > 0:
                    time.sleep(refresh_sec)
                    # chỉ START thêm worker mới trong DB (không stop worker đang chạy)
                    self.start_autostart_workers()
                else:
                    time.sleep(1)
        finally:
            self.stop_all()


def main():
    parser = argparse.ArgumentParser(description="Modbus Workers Orchestrator")
    parser.add_argument("--webapp-url", default="http://127.0.0.1:5000",
                        help="URL webapp (Socket.IO) để workers TCP emit realtime")
    parser.add_argument("--refresh-sec", type=int, default=15,
                        help="Chu kỳ (giây) đọc lại DB để auto-start worker mới; 0 = tắt")
    args = parser.parse_args()

    orch = ModbusOrchestrator(webapp_url=args.webapp_url)
    orch.run(refresh_sec=args.refresh_sec)


if __name__ == "__main__":
    main()
