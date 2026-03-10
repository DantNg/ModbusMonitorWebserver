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

# Centralized logging — tự tạo folder logs/, hoạt động cả khi build EXE
from shared.app_logger import get_logger, log_exception, log_startup
logger = get_logger("orchestra_modbus")
# ---------------- Import worker classes (nằm trong thư mục workers) ----------------
TCPWorker = None
RTUWorker = None

# Ưu tiên bản đặt tên "modbus_*_worker"
try:
    from workers.modbus_tcp_worker import ModbusTCPWorker as TCPWorker
    logger.info("Loaded TCP worker from workers.modbus_tcp_worker.ModbusTCPWorker")
except Exception as e:
    try:
        from workers.tcp_worker import TCPWorker as TCPWorker
        logger.info("Loaded TCP worker from workers.tcp_worker.TCPWorker")
    except Exception as e2:
        logger.warning("Cannot load TCP worker: %s / %s", e, e2)

try:
    from workers.modbus_rtu_worker import ModbusRTUWorker as RTUWorker
    logger.info("Loaded RTU worker from workers.modbus_rtu_worker.ModbusRTUWorker")
except Exception as e:
    try:
        from workers.rtu_worker import RTUWorker as RTUWorker
        logger.info("Loaded RTU worker from workers.rtu_worker.RTUWorker")
    except Exception as e2:
        logger.warning("Cannot load RTU worker: %s / %s", e, e2)

# ---------------- Import DB module at webapp/modbus_monitor/database/db.py ----------------
# Ưu tiên import theo đường dẫn đầy đủ để PyInstaller bắt được package khi build
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
WEBAPP_DIR = os.path.join(THIS_DIR, "webapp")
if WEBAPP_DIR not in sys.path:
    sys.path.insert(0, WEBAPP_DIR)

try:
    try:
        from webapp.modbus_monitor.database import db as db
    except Exception:
        # Khi chạy trong thư mục webapp
        from modbus_monitor.database import db as db
    logger.info("Successfully imported DB module")
except Exception as e:
    # Nếu đang chạy dưới dạng EXE, thử thêm exe_dir/webapp vào sys.path
    try:
        exe_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else None
        if exe_dir:
            wa_dir = os.path.join(exe_dir, 'webapp')
            if os.path.isdir(wa_dir) and wa_dir not in sys.path:
                sys.path.insert(0, wa_dir)
                from modbus_monitor.database import db as db
                logger.info("Imported DB module after sys.path adjustment")
                e = None
    except Exception:
        pass
    if e is not None:
        logger.critical("Cannot import DB module webapp.modbus_monitor.database.db: %s", e)
        raise

# ---------------- Import worker classes (nằm cạnh orchestra) ----------------
try:
    from workers.tcp_worker import TCPWorker
except Exception as e:
    logger.warning("Cannot load TCPWorker: %s", e)
    TCPWorker = None

try:
    from workers.rtu_worker import RTUWorker
except Exception as e:
    logger.warning("Cannot load RTUWorker: %s", e)
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
            logger.error("TCPWorker not available, skipping TCP worker")
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
            debug=True
        )
        ok = worker.start()
        if ok:
            self.workers[cfg["worker_id"]] = worker
            logger.info("TCP worker '%s' started (%s:%s)", cfg['worker_id'], host, port)
        else:
            logger.error("Failed to start TCP worker '%s'", cfg['worker_id'])

    def _start_rtu_worker(self, cfg: dict):
        if RTUWorker is None:
            logger.error("RTUWorker not available, skipping RTU worker")
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
            debug=False
        )
        ok = worker.start()
        if ok:
            self.workers[cfg["worker_id"]] = worker
            logger.info("RTU worker '%s' started (%s @ %s)", cfg['worker_id'], serial_port, baudrate)
        else:
            logger.error("Failed to start RTU worker '%s'", cfg['worker_id'])

    def start_autostart_workers(self):
        logger.info("Loading auto-start worker configs from DB ...")
        try:
            worker_rows = db.get_auto_start_workers()
        except Exception as e:
            log_exception("orchestra_modbus", e, "Failed to load auto-start workers from DB")
            return
        logger.info("Found %d auto-start workers", len(worker_rows))

        for cfg in worker_rows:
            wid = cfg["worker_id"]
            logger.debug("Processing worker: %s", wid)
            
            if not cfg.get("enabled", True):
                logger.debug("Worker '%s' disabled, skipping", wid)
                continue
                
            if not cfg.get("auto_start", True):
                logger.debug("Worker '%s' auto_start=False, skipping", wid)
                continue
                
            if wid in self.workers:
                logger.debug("Worker '%s' already running, skipping", wid)
                continue
                
            wtype = (cfg.get("worker_type") or "").lower()
            logger.info("Starting worker '%s' type '%s'", wid, wtype)
            
            try:
                if wtype == "tcp":
                    self._start_tcp_worker(cfg)
                elif wtype == "rtu":
                    self._start_rtu_worker(cfg)
                else:
                    logger.warning("Worker '%s' unsupported worker_type: %s", wid, cfg.get('worker_type'))
            except Exception as e:
                log_exception("orchestra_modbus", e, f"Error starting worker '{wid}'")
        
        logger.info("Total workers running: %d", len(self.workers))
        for worker_id, worker in self.workers.items():
            status = 'running' if worker and hasattr(worker, 'is_running') and worker.is_running else 'stopped'
            logger.info("   - %s: %s", worker_id, status)

    def stop_all(self):
        logger.info("Stopping all workers ...")
        for wid, w in list(self.workers.items()):
            try:
                w.stop()
                logger.info("Worker '%s' stopped", wid)
            except Exception as e:
                log_exception("orchestra_modbus", e, f"Error stopping worker {wid}")
        self.workers.clear()
        logger.info("All workers stopped and cleared")

    def run(self, refresh_sec: int = 15):
        def _sig_handler(signum, frame):
            logger.info("Received signal %s, shutting down ...", signum)
            self.stop_event.set()

        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)

        # Start lần đầu
        self.start_autostart_workers()
        logger.info("Hot-reload %s (interval %ds)", 'ON' if refresh_sec > 0 else 'OFF', refresh_sec)

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
    # --- Single Instance Check ---
    from utils.single_instance import ensure_single_instance
    _instance_lock = ensure_single_instance("orchestra_modbus")

    # Log system info at startup (EXE-safe)
    log_startup("orchestra_modbus", "Modbus Workers Orchestrator")

    parser = argparse.ArgumentParser(description="Modbus Workers Orchestrator")
    parser.add_argument("--webapp-url", default="http://127.0.0.1:5000",
                        help="URL webapp (Socket.IO) để workers TCP emit realtime")
    parser.add_argument("--refresh-sec", type=int, default=15,
                        help="Chu kỳ (giây) đọc lại DB để auto-start worker mới; 0 = tắt")
    args = parser.parse_args()

    try:
        orch = ModbusOrchestrator(webapp_url=args.webapp_url)
        orch.run(refresh_sec=args.refresh_sec)
    except Exception as e:
        log_exception("orchestra_modbus", e, "Fatal error in orchestra_modbus")
        raise


if __name__ == "__main__":
    main()
