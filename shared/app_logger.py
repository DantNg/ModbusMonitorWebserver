"""
Centralized Application Logger
===============================
Module logging tập trung cho toàn bộ hệ thống Modbus Monitor.
Hỗ trợ cả chế độ chạy Python script lẫn khi build thành EXE (PyInstaller).

Features:
- Tự động tạo folder logs/ cạnh file EXE hoặc project root
- RotatingFileHandler: tự xoay file khi đạt dung lượng tối đa
- Log theo từng service riêng biệt (webserver, orchestra_modbus, alarm, datalogger)
- Ghi exception kèm traceback đầy đủ
- Thread-safe, có thể gọi từ nhiều thread/process
- Format chuẩn: timestamp + level + service + message

Usage:
    from shared.app_logger import get_logger
    logger = get_logger("orchestra_modbus")
    logger.info("Worker started")
    logger.error("Connection failed", exc_info=True)

    # Hoặc dùng hàm tiện ích ghi nhanh exception:
    from shared.app_logger import log_exception
    try:
        ...
    except Exception as e:
        log_exception("orchestra_modbus", e, "Lỗi khi kết nối Modbus TCP")
"""

import logging
import os
import sys
import traceback
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional

# ========== Detect project root (works for both Python & EXE) ==========

def get_app_root() -> str:
    """
    Trả về thư mục gốc của ứng dụng.
    - Nếu chạy từ EXE (PyInstaller): thư mục chứa file .exe
    - Nếu chạy từ Python: thư mục gốc project (parent của shared/)
    """
    if getattr(sys, 'frozen', False):
        # PyInstaller EXE: exe file nằm ở thư mục nào thì dùng thư mục đó
        return os.path.dirname(sys.executable)
    else:
        # Python script: shared/ nằm trong project root
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_logs_dir() -> str:
    """
    Trả về đường dẫn tuyệt đối đến thư mục logs/.
    Tự động tạo nếu chưa tồn tại.
    """
    logs_dir = os.path.join(get_app_root(), 'logs')
    try:
        os.makedirs(logs_dir, exist_ok=True)
    except OSError as e:
        # Fallback: tạo logs/ trong thư mục hiện tại nếu không tạo được ở root
        fallback_dir = os.path.join(os.getcwd(), 'logs')
        try:
            os.makedirs(fallback_dir, exist_ok=True)
            return fallback_dir
        except OSError:
            # Nếu vẫn không được, dùng temp dir
            import tempfile
            return tempfile.gettempdir()
    return logs_dir


# ========== Logger Registry (tránh tạo trùng handler) ==========

_initialized_loggers: dict = {}

# Default configuration constants
DEFAULT_MAX_BYTES = 2 * 1024 * 1024   # 2 MB mỗi file log
DEFAULT_BACKUP_COUNT = 5               # Giữ tối đa 5 file backup
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_FORMAT = '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_logger(
    service_name: str,
    log_level: int = DEFAULT_LOG_LEVEL,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    log_to_console: bool = True,
    log_filename: Optional[str] = None,
) -> logging.Logger:
    """
    Lấy hoặc tạo logger cho một service.

    Args:
        service_name: Tên service (vd: 'orchestra_modbus', 'webserver', 'alarm_worker')
                      Cũng là tên file log: <service_name>.log
        log_level: Mức log tối thiểu (default: INFO)
        max_bytes: Dung lượng tối đa mỗi file log trước khi rotate (default: 2MB)
        backup_count: Số file backup giữ lại sau rotate (default: 5)
        log_to_console: Có in ra console không (default: True)
        log_filename: Tên file log tùy chỉnh (nếu muốn khác service_name.log)

    Returns:
        logging.Logger instance đã cấu hình sẵn
    """
    # Nếu đã tạo rồi, trả về luôn (tránh trùng handler)
    if service_name in _initialized_loggers:
        return _initialized_loggers[service_name]

    logger = logging.getLogger(service_name)
    logger.setLevel(log_level)

    # Xóa handler cũ nếu có (tránh duplicate khi auto-reload)
    if logger.handlers:
        logger.handlers.clear()

    # Formatter chuẩn
    formatter = logging.Formatter(DEFAULT_FORMAT, datefmt=DEFAULT_DATE_FORMAT)

    # ---- File Handler (RotatingFileHandler) ----
    logs_dir = get_logs_dir()
    filename = log_filename or f"{service_name}.log"
    log_file_path = os.path.join(logs_dir, filename)

    try:
        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8',
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        # Nếu không tạo được file handler, in ra console cảnh báo
        print(f"⚠️ Cannot create log file {log_file_path}: {e}")

    # ---- Console Handler ----
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # Không propagate lên root logger (tránh log trùng)
    logger.propagate = False

    # Cache lại
    _initialized_loggers[service_name] = logger

    # Log dòng đầu tiên xác nhận đã khởi tạo
    logger.info(
        "Logger initialized | logs_dir=%s | file=%s | level=%s | max_size=%s MB | backups=%d",
        logs_dir, filename, logging.getLevelName(log_level),
        round(max_bytes / (1024 * 1024), 1), backup_count
    )

    return logger


# ========== Convenience functions ==========

def log_exception(
    service_name: str,
    exception: Exception,
    context_message: str = "",
    level: int = logging.ERROR,
) -> None:
    """
    Ghi log exception kèm traceback đầy đủ.

    Args:
        service_name: Tên service ghi log
        exception: Exception object cần ghi
        context_message: Mô tả ngữ cảnh lỗi (tùy chọn)
        level: Mức log (default: ERROR)
    """
    logger = get_logger(service_name)
    tb_str = traceback.format_exception(type(exception), exception, exception.__traceback__)
    full_traceback = ''.join(tb_str)

    msg = f"{context_message} | {type(exception).__name__}: {exception}"
    logger.log(level, msg)
    logger.log(level, "Traceback:\n%s", full_traceback)


def log_system_info(service_name: str) -> None:
    """
    Ghi thông tin hệ thống khi khởi động service — hữu ích cho debug.
    """
    logger = get_logger(service_name)
    logger.info("=" * 60)
    logger.info("System Info at startup:")
    logger.info("  Python: %s", sys.version)
    logger.info("  Platform: %s", sys.platform)
    logger.info("  Frozen (EXE): %s", getattr(sys, 'frozen', False))
    logger.info("  Executable: %s", sys.executable)
    logger.info("  App Root: %s", get_app_root())
    logger.info("  Logs Dir: %s", get_logs_dir())
    logger.info("  Working Dir: %s", os.getcwd())
    logger.info("  PID: %s", os.getpid())
    logger.info("=" * 60)


def log_startup(service_name: str, description: str = "") -> logging.Logger:
    """
    Tiện ích khởi tạo logger + ghi system info khi bắt đầu service.
    Trả về logger để service dùng tiếp.

    Usage:
        logger = log_startup("orchestra_modbus", "Modbus Workers Orchestrator")
    """
    logger = get_logger(service_name)
    if description:
        logger.info("🚀 Starting: %s", description)
    log_system_info(service_name)
    return logger
