from sqlalchemy import func
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
def get_tag_values_with_interval(tag_id: int, dt_from, dt_to, interval_sec: float):
    """
    Lấy các giá trị của tag_id trong khoảng thời gian [dt_from, dt_to],
    chỉ lấy các bản ghi cách nhau >= interval_sec (giây).
    """
    with init_engine().connect() as con:
        stmt = select(
            tag_values.c.ts,
            tag_values.c.value
        ).where(
            tag_values.c.tag_id == tag_id
        )
        if dt_from:
            stmt = stmt.where(tag_values.c.ts >= dt_from)
        if dt_to:
            stmt = stmt.where(tag_values.c.ts <= dt_to)
        stmt = stmt.order_by(tag_values.c.ts.asc())
        rows = con.execute(stmt).fetchall()
        # Lọc theo interval
        filtered = []
        last_ts = None
        for ts, value in rows:
            if last_ts is None or (ts - last_ts).total_seconds() >= interval_sec:
                filtered.append((ts, value))
                last_ts = ts
        return filtered

from ast import Dict, stmt
from typing import Optional
from typing import List, Tuple
from datetime import datetime, timedelta, timezone
import os
import sys
import threading
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float, Boolean,
    DateTime, Enum, ForeignKey, select, insert, update, delete, func,cast,and_, asc, text,
    Index, UniqueConstraint
)
from sqlalchemy.engine import Engine
import json
from datetime import datetime
# ---------- Singleton Engine ----------
_engine: Optional[Engine] = None
_md = MetaData()

def safe_datetime_now():
    """Return current datetime that's compatible with database datetime comparison"""
    return datetime.now()

def safe_datetime_compare(dt1, dt2):
    """Safely compare two datetime objects handling timezone awareness"""
    if dt1 is None or dt2 is None:
        return None
    
    try:
        if dt1.tzinfo is None and dt2.tzinfo is None:
            # Both naive - direct comparison
            return dt1 - dt2
        elif dt1.tzinfo is not None and dt2.tzinfo is not None:
            # Both aware - direct comparison  
            return dt1 - dt2
        elif dt1.tzinfo is None and dt2.tzinfo is not None:
            # dt1 naive, dt2 aware - make dt1 aware (assume UTC)
            dt1_aware = dt1.replace(tzinfo=timezone.utc)
            return dt1_aware - dt2
        else:
            # dt1 aware, dt2 naive - make dt2 aware (assume UTC)
            dt2_aware = dt2.replace(tzinfo=timezone.utc)
            return dt1 - dt2_aware
    except Exception as e:
        print(f"DateTime comparison error: {e}")
        # Fallback to naive comparison
        if hasattr(dt1, 'replace') and hasattr(dt2, 'replace'):
            dt1_naive = dt1.replace(tzinfo=None) if dt1.tzinfo else dt1
            dt2_naive = dt2.replace(tzinfo=None) if dt2.tzinfo else dt2
            return dt1_naive - dt2_naive
        return None

def init_engine():
    """Khởi tạo engine một lần (singleton)."""
    global _engine
    if _engine is not None:
        return _engine
    
    # Tìm file config linh hoạt: ưu tiên cạnh executable (khi đóng gói), sau đó CWD, sau đó theo __file__, cuối cùng _MEIPASS
    config_path = None
    meipass = getattr(sys, "_MEIPASS", None)
    try:
        exe_dir = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else None
    except Exception:
        exe_dir = None

    env_path = os.environ.get("SMTP_CONFIG_PATH")
    candidates = []
    if env_path:
        candidates.append(env_path)
    if exe_dir:
        candidates.append(os.path.join(exe_dir, "config", "SMTP_config.json"))
        candidates.append(os.path.join(exe_dir, "SMTP_config.json"))
    # Current working dir
    cwd = os.getcwd()
    candidates.append(os.path.join(cwd, "config", "SMTP_config.json"))
    candidates.append(os.path.join(cwd, "SMTP_config.json"))
    # Project-root relative (when running from source)
    proj_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    candidates.append(os.path.join(proj_root, "config", "SMTP_config.json"))
    # PyInstaller temp folder
    if meipass:
        candidates.append(os.path.join(meipass, "config", "SMTP_config.json"))

    for p in candidates:
        if p and os.path.exists(p):
            config_path = p
            break

    if not config_path:
        raise FileNotFoundError("SMTP_config.json not found in expected locations (exe dir, cwd, project root, or _MEIPASS). Set SMTP_CONFIG_PATH to override.")
    
    with open(config_path) as config_file:
        config = json.load(config_file)
    uri = config.get("MYSQL_URI", "mysql+pymysql://root:123456@localhost:3306/modbus_monitor_db")
    pool_size = int(config.get("POOL_SIZE", "8"))
    _engine = create_engine(
        uri,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=pool_size,  # đủ cho vài thread poll
        future=True,
    )
    # Try to widen recipient columns to support multiple emails/phones
    try:
        with _engine.begin() as con:
            con.execute(text("ALTER TABLE alarm_rules MODIFY email VARCHAR(512) NULL"))
            con.execute(text("ALTER TABLE alarm_rules MODIFY sms VARCHAR(255) NULL"))
    except Exception as _e:
        # Ignore if table doesn't exist yet or already correct
        pass

    # Try to add created_at to subdash_group_tags for ordering tags within a group
    try:
        with _engine.begin() as con:
            con.execute(text("ALTER TABLE subdash_group_tags ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP"))
            # Backfill existing rows so ordering is consistent
            con.execute(text("UPDATE subdash_group_tags SET created_at = NOW() WHERE created_at IS NULL"))
    except Exception as _e:
        # Ignore if the column already exists or table not present yet
        pass

    # Try to add created_at to subdash_tag_groups for ordering groups by creation time
    try:
        with _engine.begin() as con:
            con.execute(text("ALTER TABLE subdash_tag_groups ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP"))
            # Backfill existing rows to current timestamp if null
            con.execute(text("UPDATE subdash_tag_groups SET created_at = NOW() WHERE created_at IS NULL"))
    except Exception as _e:
        # Ignore if the column already exists or table not present yet
        pass
    return _engine

# ---------- Schema tối giản ----------
devices = Table(
    "devices", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(120), nullable=False),
    Column("protocol", Enum("ModbusTCP", "ModbusRTU"), nullable=False, default="ModbusTCP"),
    # TCP
    Column("host", String(120)),
    Column("port", Integer),
    # RTU
    Column("serial_port", String(120)),
    Column("baudrate", Integer),
    Column("parity", String(1)),
    Column("stopbits", Integer),
    Column("bytesize", Integer),
    Column("unit_id", Integer, default=1),
    Column("timeout_ms", Integer, default=2000),
    Column("read_interval_ms", Integer, default=1000),  # Reading interval in milliseconds (default 1000ms = 1s)
    Column("byte_order", String(20), default="BigEndian"),
    Column("default_function_code", Integer, default=3),  # 1=Read Coils, 2=Read Discrete Inputs, 3=Read Holding Registers, 4=Read Input Registers
    Column("description", String(255)),
    Column("is_online", Boolean),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
)

tags = Table(
    "tags", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("device_id", Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True),
    Column("name", String(120), nullable=False),
    Column("address", Integer, nullable=False),
    Column("datatype", String(20), default="Word"),  # Changed from Enum to String to support new datatypes
    Column("unit", String(20)),
    Column("scale", Float, default=1.0),
    Column("offset", Float, default=0.0),
    Column("grp", String(50), default="Group1"),
    Column("function_code", Integer),  # Optional override: 1=Coils, 2=Discrete Inputs, 3=Holding Registers, 4=Input Registers. NULL = use device default
    Column("description", String(255)),
)

tag_values = Table(
    "tag_values", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False, index=True),
    Column("ts", DateTime, nullable=False),
    Column("value", Float, nullable=False),
)

# Bảng mới để lưu giá trị mới nhất của mỗi tag - tối ưu cho get_latest_tag_value
tag_latest_values = Table(
    "tag_latest_values", _md,
    Column("tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), primary_key=True),
    Column("value", Float, nullable=False),
    Column("ts", DateTime, nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
)

users = Table(
    "users", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("username", String(120), unique=True, nullable=False),
    Column("password_hash", String(255), nullable=False),
    Column("role", String(20), default="user"),
    Column("created_at", DateTime, server_default=func.now()),
)

alarm_rules = Table(
    "alarm_rules", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("enabled", Boolean, default=True),
    Column("code", String(50)),
    Column("name", String(120)),
    Column("level", Enum("Low", "Medium", "High", "Critical"), default="High"),
    Column("target", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),
    Column("operator", String(30)),         # >, <, >=, <=, ==, !=
    Column("threshold", String(64)),        # số hoặc "min,max"
    Column("on_stable_sec", Integer, default=0),
    Column("off_stable_sec", Integer, default=0),
    Column("email", String(120), nullable=True),  # New column for email notifications
    Column("sms", String(15), nullable=True),    # New column for SMS notifications
    Column("description", String(255), nullable=True),  # New column for alarm description
    Column("created_at", DateTime, server_default=func.now())
)
# (tuỳ chọn) Báo động tối giản – chỉ để có bảng lịch sử
alarm_events = Table(
    "alarm_events", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts", DateTime, nullable=False),
    Column("name", String(120)),
    Column("level", String(20)),
    Column("target", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),
    Column("value", Float),
    Column("note", String(255)),
    Column("event_type", Enum("INCOMING", "OUTGOING"), nullable=False),   # mới
    Column("operator", String(10)),                                       # mới
    Column("threshold", Float)                                            # mới
)
# notifications bảng để lưu thông báo cho người dùng
notifications = Table(
    "notifications", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),

    # Liên kết đến alarm event gốc
    Column("alarm_event_id", Integer, ForeignKey("alarm_events.id", ondelete="CASCADE"), nullable=False),

    # Người dùng nhận thông báo
    Column("user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False),

    # Trạng thái hiển thị
    Column("status", Enum("unread", "seen", "dismissed"), default="unread", nullable=False),

    # Dấu thời gian
    Column("seen_at", DateTime, nullable=True),
    Column("dismissed_at", DateTime, nullable=True),

    # Ghi chú tuỳ chọn (nếu muốn sau này lưu message hoặc context)
    Column("note", String(255), nullable=True),

    Column("created_at", DateTime, server_default=func.now()),

    UniqueConstraint("alarm_event_id", "user_id", name="uq_notification_user")
)

# Index để truy vấn nhanh danh sách và badge
Index("idx_notifications_user_status", notifications.c.user_id, notifications.c.status, notifications.c.created_at.desc())

# --- User Report Comparison Configs - Lưu cấu hình so sánh report theo user ---
user_report_comparison_configs = Table(
    "user_report_comparison_configs", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    Column("logger_id", String(50), nullable=False, server_default='all'),  # Logger ID hoặc 'all'
    Column("config_json", String(4000), nullable=False),  # JSON string chứa comparison pairs
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
    UniqueConstraint("user_id", "logger_id", name="uq_user_logger_comparison_config")
)

Index("idx_user_comparison_config", user_report_comparison_configs.c.user_id, user_report_comparison_configs.c.logger_id)

# --- Quad Alarm States - Lưu trạng thái alarm đang active của quad tags ---
quad_alarm_states = Table(
    "quad_alarm_states", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("quad_id", Integer, nullable=False),              # ID của quad tag card
    Column("column", String(10), nullable=False),            # "left" hoặc "right"
    Column("alarm_type", String(10), nullable=False),        # "High" hoặc "Low"
    Column("triggered_at", DateTime, nullable=False),        # Thời điểm trigger alarm
    Column("tag1_value", Float, nullable=True),              # Giá trị tag1 khi trigger
    Column("tag2_value", Float, nullable=True),              # Giá trị tag2 khi trigger
    Column("threshold", Float, nullable=True),               # Ngưỡng trigger
    Column("operator", String(10), nullable=True),           # Toán tử so sánh
    
    # Unique constraint để đảm bảo mỗi column của mỗi quad chỉ có 1 active alarm
    UniqueConstraint("quad_id", "column", name="uq_quad_alarm_state")
)

# Index để query nhanh theo quad_id
Index("idx_quad_alarm_states_quad", quad_alarm_states.c.quad_id)

# --- Bảng Data Logger ---
data_loggers = Table(
    "data_loggers", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(120), nullable=False),
    Column("interval_sec", Float, nullable=False, default=1.0),  # chu kỳ ghi dữ liệu
    Column("enabled", Boolean, default=True),
    Column("description", String(255)),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
)

# --- Bảng nối: Logger ↔ Tag ---
data_logger_tags = Table(
    "data_logger_tags", _md,
    Column("logger_id", Integer, ForeignKey("data_loggers.id", ondelete="CASCADE"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), primary_key=True),
    Column("created_at", DateTime, server_default=func.now()),
)

# --- Bảng ghi log chung ---
tag_logs = Table(
    "tag_logs", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("logger_id", Integer, ForeignKey("data_loggers.id", ondelete="CASCADE"), nullable=False),
    Column("tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),
    Column("ts", DateTime(timezone=True), nullable=False),
    Column("value", Float, nullable=False),
    Column("created_at", DateTime, server_default=func.now()),
    UniqueConstraint('logger_id', 'tag_id', 'ts', name='uq_logger_tag_ts'),
    Index('idx_logger_ts', 'logger_id', 'ts'),
    Index('idx_tag_ts', 'tag_id', 'ts'),
)

# --- Bảng: Dashboard con ---
dashboards = Table(
    "dashboards", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(120), nullable=False),
    Column("description", String(255)),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
)

# Bảng nối: dashboard ↔ tag
dashboard_tags = Table(
    "dashboard_tags", _md,
    Column("dashboard_id", Integer, ForeignKey("dashboards.id", ondelete="CASCADE"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), primary_key=True),
)


subdash_tag_groups = Table(
    "subdash_tag_groups", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("dashboard_id", Integer, ForeignKey("dashboards.id", ondelete="CASCADE"), nullable=False),
    Column("name", String(120), nullable=False),   # Tên nhóm (vd: "Temperature Zone A")
    Column("order", Integer, default=0),
    Column("created_at", DateTime, server_default=func.now()),
)

subdash_group_tags = Table(
    "subdash_group_tags", _md,
    Column("group_id", Integer, ForeignKey("subdash_tag_groups.id", ondelete="CASCADE"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), primary_key=True),
    Column("created_at", DateTime, server_default=func.now()),
)

# Bảng cho Quad Tag Cards
subdash_quad_cards = Table(
    "subdash_quad_cards", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("group_id", Integer, ForeignKey("subdash_tag_groups.id", ondelete="CASCADE"), nullable=False),
    Column("tag1_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),   # PV Left
    Column("tag2_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),   # PV Right
    Column("tag3_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # SV Left (nullable for fixed)
    Column("tag4_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # SV Right (nullable for fixed)
    # SV type/fixed value columns (fix value mode support)
    Column("sv_left_type", String(10), nullable=True, default="tag"),     # 'tag' or 'fixed'
    Column("sv_left_fixed", Float, nullable=True),
    Column("sv_right_type", String(10), nullable=True, default="tag"),
    Column("sv_right_fixed", Float, nullable=True),
    Column("card_title", String(200), nullable=True),
    Column("left_title", String(200), nullable=True),
    Column("right_title", String(200), nullable=True),
    Column("card_color", String(20), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
)

# Bảng lưu điều kiện alarm cho Quad Tag
quad_tag_conditions = Table(
    "quad_tag_conditions", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("quad_card_id", Integer, ForeignKey("subdash_quad_cards.id", ondelete="CASCADE"), nullable=False),
    Column("enabled", Boolean, default=True),  # Bật/tắt toàn bộ điều kiện
    
    # Left column conditions (tag1 & tag2)
    Column("left_high_operator", String(10)),  # >, >=, ==, !=
    Column("left_high_compare_type", String(10)),  # static hoặc tag
    Column("left_high_value", Float, nullable=True),  # Giá trị ngưỡng nếu static
    Column("left_high_compare_tag_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),  # Tag để so sánh
    Column("left_high_on_stable", Integer, default=10),  # Thời gian ổn định để kích hoạt (giây)
    Column("left_high_off_stable", Integer, default=30),  # Thời gian ổn định để tắt (giây)
    
    Column("left_low_operator", String(10)),  # <, <=, ==, !=
    Column("left_low_compare_type", String(10)),
    Column("left_low_value", Float, nullable=True),
    Column("left_low_compare_tag_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),
    Column("left_low_on_stable", Integer, default=10),
    Column("left_low_off_stable", Integer, default=30),
    
    Column("left_email", String(500), nullable=True),  # Danh sách email, phân cách bởi dấu phẩy
    Column("left_sms", String(500), nullable=True),  # Danh sách số điện thoại
    Column("left_description", String(500), nullable=True),
    
    # Right column conditions (tag3 & tag4)
    Column("right_high_operator", String(10)),
    Column("right_high_compare_type", String(10)),
    Column("right_high_value", Float, nullable=True),
    Column("right_high_compare_tag_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),
    Column("right_high_on_stable", Integer, default=10),
    Column("right_high_off_stable", Integer, default=30),
    
    Column("right_low_operator", String(10)),
    Column("right_low_compare_type", String(10)),
    Column("right_low_value", Float, nullable=True),
    Column("right_low_compare_tag_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),
    Column("right_low_on_stable", Integer, default=10),
    Column("right_low_off_stable", Integer, default=30),
    
    Column("right_email", String(500), nullable=True),
    Column("right_sms", String(500), nullable=True),
    Column("right_description", String(500), nullable=True),
    
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
)


# ========== Qtag6 Cards (6 tags: 2 columns x 3 tags each) ==========
subdash_qtag6_cards = Table(
    "subdash_qtag6_cards", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("group_id", Integer, ForeignKey("subdash_tag_groups.id", ondelete="CASCADE"), nullable=False),
    Column("tag1_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),  # Left PV
    Column("tag2_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),  # Right PV
    Column("tag3_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # Left SV HIGH (nullable for fixed)
    Column("tag4_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # Right SV HIGH (nullable for fixed)
    Column("tag5_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # Left SV LOW (nullable for fixed)
    Column("tag6_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # Right SV LOW (nullable for fixed)
    # SV type/fixed value columns (fix value mode support)
    Column("left_sv_high_type", String(10), nullable=True, default="tag"),    # 'tag' or 'fixed'
    Column("left_sv_high_fixed", Float, nullable=True),
    Column("right_sv_high_type", String(10), nullable=True, default="tag"),
    Column("right_sv_high_fixed", Float, nullable=True),
    Column("left_sv_low_type", String(10), nullable=True, default="tag"),
    Column("left_sv_low_fixed", Float, nullable=True),
    Column("right_sv_low_type", String(10), nullable=True, default="tag"),
    Column("right_sv_low_fixed", Float, nullable=True),
    Column("card_title", String(200), nullable=True),
    Column("left_title", String(200), nullable=True),
    Column("right_title", String(200), nullable=True),
    Column("card_color", String(20), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
)

# ========== Qtag4 Cards (4 tags: 2 columns x 2 tags each - PV + SV) ==========
subdash_qtag4_cards = Table(
    "subdash_qtag4_cards", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("group_id", Integer, ForeignKey("subdash_tag_groups.id", ondelete="CASCADE"), nullable=False),
    Column("tag1_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),   # Left PV
    Column("tag2_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),   # Right PV
    Column("tag3_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # Left SV (nullable for fixed)
    Column("tag4_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # Right SV (nullable for fixed)
    Column("left_sv_type", String(10), nullable=True, default="tag"),    # 'tag' or 'fixed'
    Column("left_sv_fixed", Float, nullable=True),
    Column("right_sv_type", String(10), nullable=True, default="tag"),
    Column("right_sv_fixed", Float, nullable=True),
    Column("card_title", String(200), nullable=True),
    Column("left_title", String(200), nullable=True),
    Column("right_title", String(200), nullable=True),
    Column("card_color", String(20), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
)

# ========== Qtag2 Cards (1 column: PV + SV, Qtag4-style but single column) ==========
subdash_qtag2_cards = Table(
    "subdash_qtag2_cards", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("group_id", Integer, ForeignKey("subdash_tag_groups.id", ondelete="CASCADE"), nullable=False),
    Column("tag1_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),   # PV
    Column("tag2_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # SV (nullable for fixed)
    Column("sv_type", String(10), nullable=True, default="tag"),    # 'tag' or 'fixed'
    Column("sv_fixed", Float, nullable=True),
    Column("card_title", String(200), nullable=True),
    Column("column_title", String(200), nullable=True),
    Column("card_color", String(20), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
)

# ========== Qtag3 Cards (1 column: PV + SV HIGH + SV LOW, Qtag6-style layout) ==========
subdash_qtag3_cards = Table(
    "subdash_qtag3_cards", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("group_id", Integer, ForeignKey("subdash_tag_groups.id", ondelete="CASCADE"), nullable=False),
    Column("tag1_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),   # PV
    Column("tag2_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # SV HIGH
    Column("tag3_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),   # SV LOW
    Column("sv_high_type", String(10), nullable=True, default="tag"),    # 'tag' or 'fixed'
    Column("sv_high_fixed", Float, nullable=True),
    Column("sv_low_type", String(10), nullable=True, default="tag"),     # 'tag' or 'fixed'
    Column("sv_low_fixed", Float, nullable=True),
    Column("card_title", String(200), nullable=True),
    Column("column_title", String(200), nullable=True),
    Column("card_color", String(20), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
)

# ========== Qtag Single3 Cards (PV + SV HIGH/LOW) ==========
subdash_qtag_single3_cards = Table(
    "subdash_qtag_single3_cards", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("group_id", Integer, ForeignKey("subdash_tag_groups.id", ondelete="CASCADE"), nullable=False),
    Column("card_title", String(200), nullable=True),
    Column("pv_tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),
    Column("sv_high_type", String(10), nullable=False, default="fixed"),   # 'tag' or 'fixed'
    Column("sv_high_tag_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),
    Column("sv_high_fixed", Float, nullable=True),
    Column("sv_low_type", String(10), nullable=False, default="fixed"),    # 'tag' or 'fixed'
    Column("sv_low_tag_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),
    Column("sv_low_fixed", Float, nullable=True),
    Column("card_color", String(20), nullable=True),  # Background color (hex)
    Column("created_at", DateTime, server_default=func.now()),
)

# ========== Qtag PV Only Cards (1 PV tag) ==========
subdash_qtag_pv_cards = Table(
    "subdash_qtag_pv_cards", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("group_id", Integer, ForeignKey("subdash_tag_groups.id", ondelete="CASCADE"), nullable=False),
    Column("card_title", String(200), nullable=True),
    Column("pv_tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),
    Column("card_color", String(20), nullable=True),  # Background color (hex)
    Column("created_at", DateTime, server_default=func.now()),
)

# ========== Qtag PV Dual Cards (2 PV tags, 2 columns) ==========
subdash_qtag_pv_dual_cards = Table(
    "subdash_qtag_pv_dual_cards", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("group_id", Integer, ForeignKey("subdash_tag_groups.id", ondelete="CASCADE"), nullable=False),
    Column("card_title", String(200), nullable=True),
    Column("left_title", String(200), nullable=True),
    Column("right_title", String(200), nullable=True),
    Column("left_tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),
    Column("right_tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),
    Column("card_color", String(20), nullable=True),  # Background color (hex)
    Column("created_at", DateTime, server_default=func.now()),
)

# --- Card Alarm Conditions - Unified alarm config for Qtag6, Single3, PV Only, PV Dual ---
card_alarm_conditions = Table(
    "card_alarm_conditions", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("card_type", String(20), nullable=False),  # 'qtag6', 'single3', 'pv_only', 'pv_dual'
    Column("card_id", Integer, nullable=False),        # ID in the respective card table
    Column("enabled", Boolean, default=True),

    # Left column conditions (PV tag of left column or single PV)
    Column("left_high_operator", String(10)),
    Column("left_high_compare_type", String(10)),   # 'static' or 'tag'
    Column("left_high_value", Float, nullable=True),
    Column("left_high_compare_tag_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),
    Column("left_high_on_stable", Integer, default=10),
    Column("left_high_off_stable", Integer, default=30),

    Column("left_low_operator", String(10)),
    Column("left_low_compare_type", String(10)),
    Column("left_low_value", Float, nullable=True),
    Column("left_low_compare_tag_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),
    Column("left_low_on_stable", Integer, default=10),
    Column("left_low_off_stable", Integer, default=30),

    Column("left_email", String(500), nullable=True),
    Column("left_sms", String(500), nullable=True),
    Column("left_description", String(500), nullable=True),

    # Right column conditions (only used for dual-column cards: qtag6, pv_dual)
    Column("right_high_operator", String(10)),
    Column("right_high_compare_type", String(10)),
    Column("right_high_value", Float, nullable=True),
    Column("right_high_compare_tag_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),
    Column("right_high_on_stable", Integer, default=10),
    Column("right_high_off_stable", Integer, default=30),

    Column("right_low_operator", String(10)),
    Column("right_low_compare_type", String(10)),
    Column("right_low_value", Float, nullable=True),
    Column("right_low_compare_tag_id", Integer, ForeignKey("tags.id", ondelete="SET NULL"), nullable=True),
    Column("right_low_on_stable", Integer, default=10),
    Column("right_low_off_stable", Integer, default=30),

    Column("right_email", String(500), nullable=True),
    Column("right_sms", String(500), nullable=True),
    Column("right_description", String(500), nullable=True),

    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),

    UniqueConstraint("card_type", "card_id", name="uq_card_alarm_condition")
)

Index("idx_card_alarm_conditions_type_id", card_alarm_conditions.c.card_type, card_alarm_conditions.c.card_id)

# --- Card Alarm States - Persistent alarm states for non-quad cards ---
card_alarm_states = Table(
    "card_alarm_states", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("card_type", String(20), nullable=False),
    Column("card_id", Integer, nullable=False),
    Column("column", String(10), nullable=False),           # 'left' or 'right'
    Column("alarm_type", String(10), nullable=False),       # 'High' or 'Low'
    Column("triggered_at", DateTime, nullable=False),
    Column("pv_value", Float, nullable=True),
    Column("threshold", Float, nullable=True),
    Column("operator", String(10), nullable=True),

    UniqueConstraint("card_type", "card_id", "column", name="uq_card_alarm_state")
)

Index("idx_card_alarm_states_type_id", card_alarm_states.c.card_type, card_alarm_states.c.card_id)


def create_schema():
    """Tạo bảng nếu chưa có (idempotent)."""
    engine = init_engine()
    _md.create_all(engine)
    # Migrate: add card_color column to existing tables if missing
    _migrate_card_color(engine)
    # Migrate: add fix value columns to quad and qtag6 tables
    _migrate_sv_fix_value(engine)
    # create_performance_indexes()


def _migrate_card_color(engine):
    """Add card_color column to existing card tables (safe for repeated calls)."""
    tables = ['subdash_quad_cards', 'subdash_qtag6_cards',
              'subdash_qtag4_cards', 'subdash_qtag3_cards',
              'subdash_qtag2_cards',
              'subdash_qtag_single3_cards', 'subdash_qtag_pv_cards',
              'subdash_qtag_pv_dual_cards']
    with engine.connect() as con:
        for tbl in tables:
            try:
                con.execute(text(f"SELECT card_color FROM {tbl} LIMIT 1"))
            except Exception:
                try:
                    con.execute(text(f"ALTER TABLE {tbl} ADD COLUMN card_color VARCHAR(20)"))
                    con.commit()
                    print(f"[migrate] Added card_color to {tbl}")
                except Exception as e:
                    print(f"[migrate] Could not add card_color to {tbl}: {e}")


def _migrate_sv_fix_value(engine):
    """Add SV fix value columns to quad and qtag6 tables (safe for repeated calls)."""
    migrations = {
        'subdash_quad_cards': [
            ('sv_left_type', 'VARCHAR(10)'),
            ('sv_left_fixed', 'REAL'),
            ('sv_right_type', 'VARCHAR(10)'),
            ('sv_right_fixed', 'REAL'),
        ],
        'subdash_qtag6_cards': [
            ('left_sv_high_type', 'VARCHAR(10)'),
            ('left_sv_high_fixed', 'REAL'),
            ('right_sv_high_type', 'VARCHAR(10)'),
            ('right_sv_high_fixed', 'REAL'),
            ('left_sv_low_type', 'VARCHAR(10)'),
            ('left_sv_low_fixed', 'REAL'),
            ('right_sv_low_type', 'VARCHAR(10)'),
            ('right_sv_low_fixed', 'REAL'),
        ],
    }
    with engine.connect() as con:
        for tbl, columns in migrations.items():
            for col_name, col_type in columns:
                try:
                    con.execute(text(f"SELECT {col_name} FROM {tbl} LIMIT 1"))
                except Exception:
                    try:
                        con.execute(text(f"ALTER TABLE {tbl} ADD COLUMN {col_name} {col_type}"))
                        con.commit()
                        print(f"[migrate] Added {col_name} to {tbl}")
                    except Exception as e:
                        print(f"[migrate] Could not add {col_name} to {tbl}: {e}")


# ---------- CRUD NHANH (dùng trực tiếp trong route/service) ----------
def list_devices():
    """
    List all devices from database.
    NOTE: In webapp-only mode, this function only READS data.
    Device status updates are handled by external workers.
    """
    with init_engine().connect() as con:
        rows = con.execute(select(devices)).mappings().all()
        all_devices = []
        for r in rows:
            device = dict(r)
            # In webapp-only mode, we trust the is_online status from workers
            # No automatic timeout checking or status updates here
            all_devices.append(device)
        return all_devices

def get_device(did: int):
    with init_engine().connect() as con:
        r = con.execute(select(devices).where(devices.c.id == did)).mappings().first()
        return dict(r) if r else None

def add_device_row(data: dict) -> int:
    with init_engine().begin() as con:
        res = con.execute(insert(devices).values(**data))
        return res.inserted_primary_key[0]

def list_tags(device_id: int):
    with init_engine().connect() as con:
        rows = con.execute(select(tags).where(tags.c.device_id == device_id)).mappings().all()
        return [dict(r) for r in rows]
def list_all_tags():
    """Trả về list tất cả tag kèm tên device để hiển thị chọn ở Alarm."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(
                tags.c.id.label("id"),
                tags.c.name.label("tag_name"),
                tags.c.device_id.label("device_id"),
                devices.c.name.label("device_name"),
                tags.c.address, tags.c.datatype, tags.c.unit,
            ).select_from(tags.join(devices, tags.c.device_id == devices.c.id))
             .order_by(devices.c.name.asc(), tags.c.name.asc())
        ).mappings().all()
        return [dict(r) for r in rows]
    
def add_tag_row(device_id: int, data: dict) -> int:
    data = {**data, "device_id": device_id}
    with init_engine().begin() as con:
        res = con.execute(insert(tags).values(**data))
        return res.inserted_primary_key[0]

def insert_tag_values_bulk(rows: List[Tuple[int, str, float]]):
    """
    Insert bulk data vào tag_values và update tag_latest_values
    rows = [(tag_id, ts, value), ...]
    """
    if not rows:
        return 0
    
    with init_engine().begin() as con:
        # Insert vào bảng tag_values như cũ
        con.execute(
            tag_values.insert(),
            [{"tag_id": t, "ts": ts, "value": v} for (t, ts, v) in rows]
        )
        
        # Tìm giá trị mới nhất cho mỗi tag_id từ batch này
        latest_values = {}
        for tag_id, ts, value in rows:
            if tag_id not in latest_values or ts > latest_values[tag_id][1]:
                latest_values[tag_id] = (value, ts)
        
        # Upsert vào tag_latest_values
        for tag_id, (value, ts) in latest_values.items():
            # Kiểm tra xem tag đã có trong tag_latest_values chưa
            existing = con.execute(
                select(tag_latest_values.c.ts)
                .where(tag_latest_values.c.tag_id == tag_id)
            ).first()
            
            if existing is None:
                # Insert mới
                con.execute(
                    tag_latest_values.insert().values(
                        tag_id=tag_id, value=value, ts=ts
                    )
                )
            else:
                # So sánh timestamp an toàn
                time_diff = safe_datetime_compare(ts, existing[0])
                if time_diff and time_diff.total_seconds() > 0:  # ts > existing[0]
                    # Update existing
                    con.execute(
                        update(tag_latest_values)
                        .where(tag_latest_values.c.tag_id == tag_id)
                        .values(value=value, ts=ts)
                    )
        
        return len(rows)

# Các hàm cache cũ không còn cần thiết vì sử dụng bảng tag_latest_values
# ---------- DEVICE ----------
def update_device_row(device_id: int, data: dict) -> int:
    # loại bỏ key None để không overwrite
    print("🔄 Updating device:", device_id, data)
    data = {k: v for k, v in data.items() if k in devices.c and v is not None}
    with init_engine().begin() as con:
        res = con.execute(update(devices).where(devices.c.id == device_id).values(**data))
        return res.rowcount

def get_all_device_statuses_from_db() -> dict:
    """
    Lấy trạng thái tất cả devices từ database
    Returns: dict {device_id: {"is_online": bool, "updated_at": datetime, ...}}
    """
    try:
        with init_engine().connect() as con:
            result = con.execute(
                select(devices.c.id, devices.c.name, devices.c.is_online, devices.c.updated_at, devices.c.created_at)
                .order_by(devices.c.id)
            ).fetchall()
            
            statuses = {}
            for row in result:
                statuses[row[0]] = {  # device_id as key
                    "device_id": row[0],
                    "name": row[1],
                    "is_online": row[2],
                    "updated_at": row[3],
                    "created_at": row[4]
                }
            
            print(f"📊 Loaded {len(statuses)} device statuses from database")
            return statuses
            
    except Exception as e:
        print(f"❌ Error loading device statuses from DB: {e}")
        return {}

def delete_device_row(device_id: int) -> int:
    with init_engine().begin() as con:
        # Find all tag IDs for this device
        tag_ids = [r["id"] for r in con.execute(select(tags.c.id).where(tags.c.device_id == device_id)).mappings().all()]
        if tag_ids:
            # Find all logger IDs that only reference these tags
            logger_ids = [r["logger_id"] for r in con.execute(
                select(data_logger_tags.c.logger_id)
                .where(data_logger_tags.c.tag_id.in_(tag_ids))
            ).mappings().all()]
            if logger_ids:
                # Delete loggers (will cascade to data_logger_tags)
                con.execute(delete(data_loggers).where(data_loggers.c.id.in_(logger_ids)))
        # Delete device (will cascade to tags and tag_values)
        res = con.execute(delete(devices).where(devices.c.id == device_id))
        return res.rowcount

def update_device_status_by_tag(tag_id,status):
    """
    Update the 'is_online' field for the device associated with the given tag_id.
    A device is considered online if the tag's last_updated timestamp is within the last 60 seconds.
    :param tag_id: ID of the tag
    """
    # Fetch the tag details
    tag = get_tag(tag_id)
    if not tag:
        print(f"Tag with ID {tag_id} not found.")
        return

    # Fetch the device associated with the tag
    device_id = tag.get("device_id")
    if not device_id:
        print(f"No device associated with tag ID {tag_id}.")
        return
    update_device_row(device_id, {"is_online": status, "updated_at": safe_datetime_now()})

def sync_device_status_to_mysql():
    """
    Đồng bộ trạng thái device từ config_cache vào MySQL table devices
    Cập nhật is_online và updated_at cho từng device
    """
    try:
        from modbus_monitor.services.config_cache import get_config_cache
        config_cache = get_config_cache()
        
        # Lấy tất cả device statuses từ config_cache
        device_statuses = config_cache.get_all_device_statuses()
        
        if not device_statuses:
            print("⚠️ No devices found in config_cache - skipping sync")
            return {
                "success": True,
                "updated_count": 0,
                "skipped_count": 0,
                "total_processed": 0,
                "message": "No devices to sync"
            }
        
        # Cập nhật MySQL database
        with init_engine().begin() as con:
            updated_count = 0
            skipped_count = 0
            
            for device_id, status_info in device_statuses.items():
                try:
                    device_status = status_info.get('status', 'unknown')
                    last_seen = status_info.get('last_seen')
                    
                    # Chuyển đổi status từ cache sang is_online boolean
                    # connected = True, disconnected/unknown = False
                    is_online = (device_status == 'connected')
                    
                    # Cập nhật MySQL table devices
                    res = con.execute(
                        update(devices)
                        .where(devices.c.id == device_id)
                        .values(
                            is_online=is_online,
                            updated_at=safe_datetime_now()
                        )
                    )
                    
                    if res.rowcount > 0:
                        updated_count += 1
                        # print(f"✅ Device {device_id}: MySQL synced - {device_status} -> is_online={is_online}")
                    else:
                        skipped_count += 1
                        print(f"⚠️ Device {device_id}: Not found in MySQL table")
                        
                except Exception as device_error:
                    skipped_count += 1
                    print(f"❌ Error syncing device {device_id}: {device_error}")
                    continue
            
            # Summary log
            total_processed = len(device_statuses)
            # print(f"📊 MySQL Sync Summary: {updated_count} updated, {skipped_count} skipped, {total_processed} total")
            
            return {
                "success": True,
                "updated_count": updated_count,
                "skipped_count": skipped_count,
                "total_processed": total_processed,
                "message": f"Sync completed: {updated_count}/{total_processed} devices updated"
            }
            
    except Exception as e:
        error_msg = f"MySQL device sync failed: {e}"
        print(f"❌ {error_msg}")
        return {
            "success": False,
            "error": error_msg,
            "updated_count": 0,
            "skipped_count": 0,
            "total_processed": 0
        }

def get_device_status_comparison():
    """
    So sánh trạng thái device giữa config_cache và MySQL database
    Để debug và kiểm tra sự đồng bộ
    """
    try:
        from modbus_monitor.services.config_cache import get_config_cache
        config_cache = get_config_cache()
        
        # Lấy status từ config_cache
        cache_statuses = config_cache.get_all_device_statuses()
        
        # Lấy status từ MySQL
        with init_engine().connect() as con:
            db_devices = con.execute(
                select(devices.c.id, devices.c.name, devices.c.is_online, devices.c.updated_at)
            ).mappings().all()
        
        comparison = []
        for db_device in db_devices:
            device_id = db_device['id']
            cache_status = cache_statuses.get(device_id, {})
            
            cache_online = cache_status.get('status') == 'connected' if cache_status else None
            db_online = db_device['is_online']
            
            is_synced = (cache_online == db_online) if cache_online is not None else False
            
            comparison.append({
                "device_id": device_id,
                "device_name": db_device['name'],
                "cache_status": cache_status.get('status', 'not_found'),
                "cache_online": cache_online,
                "db_online": db_online,
                "is_synced": is_synced,
                "db_updated_at": db_device['updated_at']
            })
        
        return {
            "success": True,
            "comparison": comparison,
            "total_devices": len(comparison),
            "synced_count": sum(1 for item in comparison if item['is_synced']),
            "unsynced_count": sum(1 for item in comparison if not item['is_synced'])
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
# ---------- TAG ----------
def get_tag(tag_id: int):
    with init_engine().connect() as con:
        r = con.execute(select(tags).where(tags.c.id == tag_id)).mappings().first()
        return dict(r) if r else None

def update_tag_row(tag_id: int, data: dict) -> int:
    data = {k: v for k, v in data.items() if k in tags.c and v is not None}
    with init_engine().begin() as con:
        res = con.execute(update(tags).where(tags.c.id == tag_id).values(**data))
        return res.rowcount

def delete_tag_row(tag_id: int) -> int:
    with init_engine().begin() as con:
        res = con.execute(delete(tags).where(tags.c.id == tag_id))
        return res.rowcount

#------------ ALARM ----------------
def list_alarm_rules():
    with init_engine().connect() as con:
        rows = con.execute(select(alarm_rules).order_by(alarm_rules.c.id.desc())).mappings().all()
        return [dict(r) for r in rows]

def get_alarm_rule(aid: int):
    with init_engine().connect() as con:
        r = con.execute(select(alarm_rules).where(alarm_rules.c.id == aid)).mappings().first()
        return dict(r) if r else None

def add_alarm_rule_row(data: dict) -> int:
    with init_engine().begin() as con:
        res = con.execute(insert(alarm_rules).values(**data))
        return res.inserted_primary_key[0]

def update_alarm_rule_row(aid: int, data: dict) -> int:
    data = {k: v for k, v in data.items() if k in alarm_rules.c and v is not None}
    with init_engine().begin() as con:
        res = con.execute(update(alarm_rules).where(alarm_rules.c.id == aid).values(**data))
        return res.rowcount

def delete_alarm_rule_row(aid: int) -> int:
    with init_engine().begin() as con:
        res = con.execute(delete(alarm_rules).where(alarm_rules.c.id == aid))
        return res.rowcount

def list_alarm_rules_for_device(device_id: int):
    """
    Trả về các alarm rule áp dụng cho các tag thuộc device_id.
    alarm_rules.target đang lưu tag_id (string), nên cần cast về Integer để join với tags.id.
    """
    with init_engine().connect() as con:
        rows = con.execute(
            select(
                alarm_rules.c.id,
                alarm_rules.c.name,               # Thêm name field
                alarm_rules.c.target,             # tag_id (string)
                alarm_rules.c.operator,
                alarm_rules.c.threshold,
                alarm_rules.c.on_stable_sec,
                alarm_rules.c.off_stable_sec,
                alarm_rules.c.enabled,
                alarm_rules.c.level,
                alarm_rules.c.email,
                alarm_rules.c.sms,
                tags.c.name.label("tag_name")    # Thêm tag name để debug
            )
            .select_from(
                alarm_rules.join(
                    tags, cast(alarm_rules.c.target, Integer) == tags.c.id
                )
            )
            .where(tags.c.device_id == device_id)
            .order_by(alarm_rules.c.id.desc())
        ).mappings().all()
        return [dict(r) for r in rows]
 
def insert_alarm_event(ts, name, level, target, value, note="", event_type="INCOMING", operator=">", threshold=0.0):
    with init_engine().begin() as con:
        res = con.execute(
            alarm_events.insert().values(
                ts=ts,
                name=name,
                level=level,
                target=target,
                value=value,
                note=note,
                event_type=event_type,
                operator=operator,
                threshold=threshold
            )
        )
        # Try to retrieve the inserted primary key from the executed result
        inserted_id = None
        try:
            inserted_id = res.inserted_primary_key[0]
        except Exception:
            inserted_id = None

    # If we got an inserted id, create notifications for it
    if inserted_id is not None:
        insert_notification(inserted_id)
    return inserted_id

# ----------- ALARM EVENTS (history) -----------
def get_distinct_alarm_names():
    """Return distinct alarm names from alarm_events table for autocomplete."""
    with init_engine().connect() as con:
        query = select(alarm_events.c.name).distinct().where(
            alarm_events.c.name.isnot(None)
        ).order_by(alarm_events.c.name.asc())
        rows = con.execute(query).all()
        return [r[0] for r in rows if r[0]]

def list_alarm_events(offset: int = 0, limit: int = None, alarm_name: str = None):
    """Return all alarm events with pagination, oldest first (ascending by timestamp).
    Optional alarm_name filter to search by exact alarm name.
    Returns: (items, total_count)
    """
    with init_engine().connect() as con:
        # Base filter conditions
        conditions = []
        if alarm_name:
            conditions.append(alarm_events.c.name == alarm_name)
        
        # Get total count first
        count_q = select(func.count()).select_from(alarm_events)
        for cond in conditions:
            count_q = count_q.where(cond)
        count_result = con.execute(count_q).scalar()
        total_count = count_result or 0
        
        # Build query with pagination
        query = select(alarm_events).order_by(alarm_events.c.ts.asc())
        for cond in conditions:
            query = query.where(cond)
        if offset > 0:
            query = query.offset(offset)
        if limit is not None:
            query = query.limit(limit)
        
        rows = con.execute(query).mappings().all()
        return [dict(r) for r in rows], total_count

def list_alarm_events_by_date_range(start_date, end_date, offset: int = 0, limit: int = None, alarm_name: str = None):
    """Return alarm events within date range with pagination, oldest first (ascending by timestamp).
    Optional alarm_name filter to search by exact alarm name.
    Returns: (items, total_count)
    """
    with init_engine().connect() as con:
        # Build filter conditions
        conditions = [
            alarm_events.c.ts >= start_date,
            alarm_events.c.ts <= end_date
        ]
        if alarm_name:
            conditions.append(alarm_events.c.name == alarm_name)
        
        # Get total count first
        count_query = select(func.count()).select_from(alarm_events)
        for cond in conditions:
            count_query = count_query.where(cond)
        count_result = con.execute(count_query).scalar()
        total_count = count_result or 0
        
        # Build query with pagination
        query = select(alarm_events).order_by(alarm_events.c.ts.asc())
        for cond in conditions:
            query = query.where(cond)
        
        if offset > 0:
            query = query.offset(offset)
        if limit is not None:
            query = query.limit(limit)
        
        rows = con.execute(query).mappings().all()
        return [dict(r) for r in rows], total_count

def clear_alarm_events():
    """Delete all alarm events."""
    with init_engine().begin() as con:
        res = con.execute(delete(alarm_events))
        return res.rowcount

def clear_report_data():
    """Delete all datalogger report data from tag_logs table."""
    with init_engine().begin() as con:
        res = con.execute(delete(tag_logs))
        return res.rowcount

def clear_report_data_by_logger(logger_id: int):
    """Delete datalogger report data for specific logger from tag_logs table."""
    with init_engine().begin() as con:
        # Delete all tag_logs entries for this logger_id
        res = con.execute(delete(tag_logs).where(tag_logs.c.logger_id == logger_id))
        return res.rowcount
# Delete a single alarm event by id
def delete_alarm_event_row(eid: int) -> int:
    with init_engine().begin() as con:
        res = con.execute(delete(alarm_events).where(alarm_events.c.id == eid))
        return res.rowcount
def delete_alarm_events(ids: List[int]) -> int:
    with init_engine().begin() as con:
        res = con.execute(delete(alarm_events).where(alarm_events.c.id.in_(ids)))
        return res.rowcount

# ----------- QUAD ALARM STATES -----------
def insert_quad_alarm_state(quad_id: int, column: str, alarm_type: str, 
                           tag1_value: float, tag2_value: float, 
                           threshold: float, operator: str):
    """Insert or update quad alarm state (UPSERT)"""
    try:
        with init_engine().begin() as con:
            # Try to delete existing state first, then insert new one
            con.execute(
                delete(quad_alarm_states)
                .where(quad_alarm_states.c.quad_id == quad_id)
                .where(quad_alarm_states.c.column == column)
            )
            
            # Insert new state
            res = con.execute(
                quad_alarm_states.insert().values(
                    quad_id=quad_id,
                    column=column,
                    alarm_type=alarm_type,
                    triggered_at=datetime.now(),
                    tag1_value=tag1_value,
                    tag2_value=tag2_value,
                    threshold=threshold,
                    operator=operator
                )
            )
            return res.inserted_primary_key[0] if res.inserted_primary_key else None
    except Exception as e:
        print(f"Error inserting quad alarm state: {e}")
        return None

def delete_quad_alarm_state(quad_id: int, column: str):
    """Delete quad alarm state when alarm clears"""
    try:
        with init_engine().begin() as con:
            res = con.execute(
                delete(quad_alarm_states)
                .where(quad_alarm_states.c.quad_id == quad_id)
                .where(quad_alarm_states.c.column == column)
            )
            return res.rowcount
    except Exception as e:
        print(f"Error deleting quad alarm state: {e}")
        return 0

def get_active_quad_alarms():
    """Get all active quad alarms"""
    try:
        with init_engine().connect() as con:
            rows = con.execute(
                select(quad_alarm_states).order_by(quad_alarm_states.c.triggered_at.desc())
            ).mappings().all()
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error getting active quad alarms: {e}")
        return []

def get_active_quad_alarms_by_subdash(subdash_id: int):
    """Get active quad alarms for a specific subdashboard"""
    try:
        # Get all quad cards for this subdashboard first
        quad_card_ids = []
        with init_engine().connect() as con:
            # Get quad card IDs through subdash_tag_groups
            query = select(subdash_quad_cards.c.id).select_from(
                subdash_quad_cards.join(
                    subdash_tag_groups,
                    subdash_quad_cards.c.group_id == subdash_tag_groups.c.id
                )
            ).where(subdash_tag_groups.c.dashboard_id == subdash_id)
            
            rows = con.execute(query).fetchall()
            quad_card_ids = [row[0] for row in rows]
        
        if not quad_card_ids:
            return []
        
        # Now get active alarms for these quad cards
        with init_engine().connect() as con:
            query = select(
                quad_alarm_states.c.quad_id,
                quad_alarm_states.c.column,
                quad_alarm_states.c.alarm_type,
                quad_alarm_states.c.triggered_at,
                quad_alarm_states.c.tag1_value,
                quad_alarm_states.c.tag2_value,
                quad_alarm_states.c.threshold,
                quad_alarm_states.c.operator
            ).where(
                quad_alarm_states.c.quad_id.in_(quad_card_ids)
            ).order_by(quad_alarm_states.c.triggered_at.desc())
            
            rows = con.execute(query).mappings().all()
            result = [dict(r) for r in rows]
            print(f"🔔 Loaded {len(result)} active quad alarms for subdash {subdash_id}: {result}")
            return result
    except Exception as e:
        print(f"Error getting active quad alarms by subdash: {e}")
        import traceback
        traceback.print_exc()
        return []


def get_active_tag_alarms_for_subdash(subdash_id: int):
    """Get currently active per-tag alarm states for all tags used in Qtag cards of a subdashboard.
    
    A tag alarm is 'active' when the latest alarm_event for that tag is INCOMING.
    Uses the existing alarm_events table - no new tables needed.
    
    Returns list of dicts: [{tag_id, alarm_name, level, value, operator, threshold, triggered_at}, ...]
    """
    try:
        with init_engine().connect() as con:
            # Step 1: Collect all tag IDs used in qtag6, single3, pv cards for this subdashboard
            all_tag_ids = set()

            # Qtag6: 6 tags per card
            qtag6_rows = con.execute(
                select(subdash_qtag6_cards)
                .select_from(
                    subdash_qtag6_cards.join(
                        subdash_tag_groups,
                        subdash_qtag6_cards.c.group_id == subdash_tag_groups.c.id
                    )
                )
                .where(subdash_tag_groups.c.dashboard_id == subdash_id)
            ).mappings().all()
            for card in qtag6_rows:
                for pos in range(1, 7):
                    tid = card.get(f'tag{pos}_id')
                    if tid:
                        all_tag_ids.add(tid)

            # Single3: PV tag + optional SV HIGH/LOW tags
            single3_rows = con.execute(
                select(subdash_qtag_single3_cards)
                .select_from(
                    subdash_qtag_single3_cards.join(
                        subdash_tag_groups,
                        subdash_qtag_single3_cards.c.group_id == subdash_tag_groups.c.id
                    )
                )
                .where(subdash_tag_groups.c.dashboard_id == subdash_id)
            ).mappings().all()
            for card in single3_rows:
                if card.get('pv_tag_id'):
                    all_tag_ids.add(card['pv_tag_id'])
                if card.get('sv_high_tag_id'):
                    all_tag_ids.add(card['sv_high_tag_id'])
                if card.get('sv_low_tag_id'):
                    all_tag_ids.add(card['sv_low_tag_id'])

            # PV Only: 1 PV tag per card
            pv_rows = con.execute(
                select(subdash_qtag_pv_cards)
                .select_from(
                    subdash_qtag_pv_cards.join(
                        subdash_tag_groups,
                        subdash_qtag_pv_cards.c.group_id == subdash_tag_groups.c.id
                    )
                )
                .where(subdash_tag_groups.c.dashboard_id == subdash_id)
            ).mappings().all()
            for card in pv_rows:
                if card.get('pv_tag_id'):
                    all_tag_ids.add(card['pv_tag_id'])

            # PV Dual: 2 PV tags per card (left + right)
            pv_dual_rows = con.execute(
                select(subdash_qtag_pv_dual_cards)
                .select_from(
                    subdash_qtag_pv_dual_cards.join(
                        subdash_tag_groups,
                        subdash_qtag_pv_dual_cards.c.group_id == subdash_tag_groups.c.id
                    )
                )
                .where(subdash_tag_groups.c.dashboard_id == subdash_id)
            ).mappings().all()
            for card in pv_dual_rows:
                if card.get('left_tag_id'):
                    all_tag_ids.add(card['left_tag_id'])
                if card.get('right_tag_id'):
                    all_tag_ids.add(card['right_tag_id'])

            # Also include tags from quad cards (tag1..tag4)
            quad_rows = con.execute(
                select(subdash_quad_cards)
                .select_from(
                    subdash_quad_cards.join(
                        subdash_tag_groups,
                        subdash_quad_cards.c.group_id == subdash_tag_groups.c.id
                    )
                )
                .where(subdash_tag_groups.c.dashboard_id == subdash_id)
            ).mappings().all()
            for card in quad_rows:
                for pos in range(1, 5):
                    tid = card.get(f'tag{pos}_id')
                    if tid:
                        all_tag_ids.add(tid)

            if not all_tag_ids:
                return []

            # Step 2: For each tag, find latest alarm_event and check if it's INCOMING
            # Use a subquery to get the max event ID per tag (latest event)
            from sqlalchemy import func as sa_func
            subq = (
                select(
                    alarm_events.c.target,
                    sa_func.max(alarm_events.c.id).label('max_id')
                )
                .where(alarm_events.c.target.in_(list(all_tag_ids)))
                .group_by(alarm_events.c.target)
                .subquery()
            )

            # Join back to get full event details, filter only INCOMING
            rows = con.execute(
                select(
                    alarm_events.c.target.label('tag_id'),
                    alarm_events.c.name.label('alarm_name'),
                    alarm_events.c.level,
                    alarm_events.c.value,
                    alarm_events.c.operator,
                    alarm_events.c.threshold,
                    alarm_events.c.ts.label('triggered_at'),
                    alarm_events.c.event_type
                )
                .select_from(
                    alarm_events.join(
                        subq,
                        (alarm_events.c.target == subq.c.target) &
                        (alarm_events.c.id == subq.c.max_id)
                    )
                )
                .where(alarm_events.c.event_type == 'INCOMING')
            ).mappings().all()

            result = [dict(r) for r in rows]
            return result

    except Exception as e:
        print(f"Error getting active tag alarms for subdash {subdash_id}: {e}")
        import traceback
        traceback.print_exc()
        return []

    
def list_alarm_report(alarm_name=None):
    """Ghép INCOMING với OUTGOING + rule info để làm báo cáo alarm."""
    print("Generating alarm report...")
    items = []
    tag_names = {}
    quad_titles = {}
    # Preload tag names and quad card titles for export naming
    try:
        with init_engine().connect() as con:
            tag_rows = con.execute(select(tags.c.id, tags.c.name)).mappings().all()
            tag_names = {r["id"]: r["name"] for r in tag_rows}

            quad_rows = con.execute(
                select(
                    subdash_quad_cards.c.card_title,
                    subdash_quad_cards.c.left_title,
                    subdash_quad_cards.c.right_title,
                    subdash_quad_cards.c.tag1_id,
                    subdash_quad_cards.c.tag2_id,
                    subdash_quad_cards.c.tag3_id,
                    subdash_quad_cards.c.tag4_id,
                )
            ).mappings().all()

            for qr in quad_rows:
                # Map tag to quad card title + column title
                if qr.get("tag1_id") is not None:
                    quad_titles[qr["tag1_id"]] = f"{qr.get('card_title') or 'Quad'} - {qr.get('left_title') or 'Left'}"
                if qr.get("tag3_id") is not None:
                    quad_titles[qr["tag3_id"]] = f"{qr.get('card_title') or 'Quad'} - {qr.get('left_title') or 'Left'}"
                if qr.get("tag2_id") is not None:
                    quad_titles[qr["tag2_id"]] = f"{qr.get('card_title') or 'Quad'} - {qr.get('right_title') or 'Right'}"
                if qr.get("tag4_id") is not None:
                    quad_titles[qr["tag4_id"]] = f"{qr.get('card_title') or 'Quad'} - {qr.get('right_title') or 'Right'}"
    except Exception as e:
        print(f"⚠️ Cannot preload tag/quad names: {e}")
    with init_engine().connect() as con:
        q_in = select(
            alarm_events.c.id,
            alarm_events.c.ts.label("incoming_date"),
            alarm_events.c.value.label("incoming_value"),
            alarm_events.c.target,
            alarm_events.c.name,
            alarm_events.c.level,
            alarm_events.c.operator.label("op"),
            alarm_events.c.threshold.label("th"),
        ).where(alarm_events.c.event_type == "INCOMING")

        # Filter by alarm name at SQL level if specified
        if alarm_name:
            q_in = q_in.where(alarm_events.c.name == alarm_name)

        incoming_rows = con.execute(q_in).mappings().all()
        for inc in incoming_rows:
            q_out = select(
                alarm_events.c.ts.label("outgoing_date"),
                alarm_events.c.value.label("outgoing_value")
            ).where(
                and_(
                    alarm_events.c.target == inc["target"],
                    alarm_events.c.event_type == "OUTGOING",
                    alarm_events.c.ts > inc["incoming_date"]
                )
            ).order_by(alarm_events.c.ts.asc()).limit(1)

            out = con.execute(q_out).mappings().first()

            # Prefer quad title if tag belongs to quad, else tag name, else stored event name
            display_name = quad_titles.get(inc["target"]) or tag_names.get(inc["target"], inc["name"] or "")

            items.append({
                "acknowledged": False,
                "code": display_name,
                "level": inc["level"],
                "incoming_date": inc["incoming_date"].strftime("%d/%m/%Y %H:%M:%S"),
                "incoming_value": inc["incoming_value"],
                "outgoing_date": out["outgoing_date"].strftime("%d/%m/%Y %H:%M:%S") if out else "",
                "outgoing_value": out["outgoing_value"] if out else "",
                "operator": inc["op"] if inc["op"] is not None else "",
                "threshold": inc["th"] if inc["th"] is not None else "",
            })
    print(f"Generated {len(items)} alarm report items.")
    return items

def list_alarm_report_by_date_range(start_date, end_date, alarm_name=None):
    """Generate alarm report but only for INCOMING events within the given date range.
    OUTGOING is matched as the first event after INCOMING (may be outside range)."""
    print(f"Generating alarm report for range: {start_date} -> {end_date}")
    items = []
    tag_names = {}
    quad_titles = {}
    # Preload tag and quad names for correct export labels
    try:
        with init_engine().connect() as con:
            tag_rows = con.execute(select(tags.c.id, tags.c.name)).mappings().all()
            tag_names = {r["id"]: r["name"] for r in tag_rows}

            quad_rows = con.execute(
                select(
                    subdash_quad_cards.c.card_title,
                    subdash_quad_cards.c.left_title,
                    subdash_quad_cards.c.right_title,
                    subdash_quad_cards.c.tag1_id,
                    subdash_quad_cards.c.tag2_id,
                    subdash_quad_cards.c.tag3_id,
                    subdash_quad_cards.c.tag4_id,
                )
            ).mappings().all()

            for qr in quad_rows:
                if qr.get("tag1_id") is not None:
                    quad_titles[qr["tag1_id"]] = f"{qr.get('card_title') or 'Quad'} - {qr.get('left_title') or 'Left'}"
                if qr.get("tag3_id") is not None:
                    quad_titles[qr["tag3_id"]] = f"{qr.get('card_title') or 'Quad'} - {qr.get('left_title') or 'Left'}"
                if qr.get("tag2_id") is not None:
                    quad_titles[qr["tag2_id"]] = f"{qr.get('card_title') or 'Quad'} - {qr.get('right_title') or 'Right'}"
                if qr.get("tag4_id") is not None:
                    quad_titles[qr["tag4_id"]] = f"{qr.get('card_title') or 'Quad'} - {qr.get('right_title') or 'Right'}"
    except Exception as e:
        print(f"⚠️ Cannot preload tag/quad names: {e}")
    with init_engine().connect() as con:
        q_in = select(
            alarm_events.c.id,
            alarm_events.c.ts.label("incoming_date"),
            alarm_events.c.value.label("incoming_value"),
            alarm_events.c.target,
            alarm_events.c.name,
            alarm_events.c.level,
            alarm_events.c.operator.label("op"),
            alarm_events.c.threshold.label("th"),
        ).where(
            and_(
                alarm_events.c.event_type == "INCOMING",
                alarm_events.c.ts >= start_date,
                alarm_events.c.ts <= end_date,
            )
        )

        # Filter by alarm name at SQL level if specified
        if alarm_name:
            q_in = q_in.where(alarm_events.c.name == alarm_name)

        incoming_rows = con.execute(q_in).mappings().all()
        for inc in incoming_rows:
            q_out = select(
                alarm_events.c.ts.label("outgoing_date"),
                alarm_events.c.value.label("outgoing_value")
            ).where(
                and_(
                    alarm_events.c.target == inc["target"],
                    alarm_events.c.event_type == "OUTGOING",
                    alarm_events.c.ts > inc["incoming_date"]
                )
            ).order_by(alarm_events.c.ts.asc()).limit(1)

            out = con.execute(q_out).mappings().first()

            display_name = quad_titles.get(inc["target"]) or tag_names.get(inc["target"], inc["name"] or "")

            items.append({
                "acknowledged": False,
                "code": display_name,
                "level": inc["level"],
                "incoming_date": inc["incoming_date"].strftime("%d/%m/%Y %H:%M:%S"),
                "incoming_value": inc["incoming_value"],
                "outgoing_date": out["outgoing_date"].strftime("%d/%m/%Y %H:%M:%S") if out else "",
                "outgoing_value": out["outgoing_value"] if out else "",
                "operator": inc["op"] if inc["op"] is not None else "",
                "threshold": inc["th"] if inc["th"] is not None else "",
            })
    print(f"Generated {len(items)} alarm report items (filtered).")
    return items
#---------- DATA LOGGER ----------------
def list_data_loggers():
    """Danh sách logger + số tag đính kèm."""
    with init_engine().connect() as con:
        # đếm số tag trên từng logger
        count_rows = con.execute(
            select(
                data_logger_tags.c.logger_id,
                func.count(data_logger_tags.c.tag_id).label("tag_count")
            ).group_by(data_logger_tags.c.logger_id)
        ).mappings().all()
        counts = {r["logger_id"]: r["tag_count"] for r in count_rows}

        rows = con.execute(select(data_loggers).order_by(data_loggers.c.id.desc())).mappings().all()
        res = []
        for r in rows:
            d = dict(r)
            d["tag_count"] = counts.get(d["id"], 0)
            res.append(d)
        return res

def get_data_logger(lid: int):
    with init_engine().connect() as con:
        r = con.execute(select(data_loggers).where(data_loggers.c.id == lid)).mappings().first()
        return dict(r) if r else None

def get_data_logger_tag_ids(lid: int) -> List[int]:
    """Return tag IDs for a logger in the order they were attached (older first)."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(data_logger_tags.c.tag_id)
            .where(data_logger_tags.c.logger_id == lid)
            .order_by(data_logger_tags.c.created_at.asc(), data_logger_tags.c.tag_id.asc())
        ).all()
        return [r[0] for r in rows]

# Alias for backward compatibility
def list_data_logger_tags(lid: int) -> List[int]:
    """Alias for get_data_logger_tag_ids"""
    return get_data_logger_tag_ids(lid)

def add_data_logger_row(data: dict, tag_ids: List[int]) -> int:
    with init_engine().begin() as con:
        res = con.execute(insert(data_loggers).values(**data))
        new_id = res.inserted_primary_key[0]
        if tag_ids:
            con.execute(
                data_logger_tags.insert(),
                [{"logger_id": new_id, "tag_id": tid} for tid in tag_ids]
            )
        return new_id

def update_data_logger_row(lid: int, data: dict, tag_ids: List[int]) -> int:
    """Cập nhật logger và cập nhật mapping tag theo kiểu diff để KHÔNG reset created_at của tag đã tồn tại.

    - Chỉ INSERT những tag mới được thêm vào (giữ created_at theo mặc định now())
    - Chỉ DELETE những tag bị gỡ bỏ
    - Giữ nguyên các hàng mapping hiện có để preserved created_at
    """
    # Dedupe input order while preserving order
    tag_ids = tag_ids or []
    seen = set()
    dedup_tag_ids = []
    for tid in tag_ids:
        if tid not in seen:
            seen.add(tid)
            dedup_tag_ids.append(tid)

    with init_engine().begin() as con:
        # Update logger info first
        res = con.execute(
            update(data_loggers).where(data_loggers.c.id == lid).values(**data)
        )

        # Get existing mapping
        existing_ids = [
            r[0] for r in con.execute(
                select(data_logger_tags.c.tag_id).where(data_logger_tags.c.logger_id == lid)
            ).all()
        ]

        existing_set = set(existing_ids)
        desired_set = set(dedup_tag_ids)

        to_add = [tid for tid in dedup_tag_ids if tid not in existing_set]
        to_remove = [tid for tid in existing_ids if tid not in desired_set]

        # Apply removals (only those not desired anymore)
        if to_remove:
            con.execute(
                delete(data_logger_tags).where(
                    and_(
                        data_logger_tags.c.logger_id == lid,
                        data_logger_tags.c.tag_id.in_(to_remove),
                    )
                )
            )

        # Apply additions (let created_at default be now())
        if to_add:
            con.execute(
                data_logger_tags.insert(),
                [{"logger_id": lid, "tag_id": tid} for tid in to_add]
            )

        return res.rowcount

def delete_data_logger_row(lid: int) -> int:
    with init_engine().begin() as con:
        # nhờ ON DELETE CASCADE ở FK nên xóa logger sẽ cuốn luôn mapping
        res = con.execute(delete(data_loggers).where(data_loggers.c.id == lid))
        return res.rowcount
def get_logger_rows(logger_id: int, dt_from: datetime = None, dt_to: datetime = None):
    with init_engine().begin() as con:
        # Lấy danh sách tag_id của logger này
        tag_ids = [
            row.tag_id for row in con.execute(
                select(data_logger_tags.c.tag_id)
                .where(data_logger_tags.c.logger_id == logger_id)
            )
        ]
        if not tag_ids:
            return [], ["timestamp"]
        # print(f"Logger {logger_id} has tags: {tag_ids}")
        # Lấy tên tag
        tags_map = {
            row.id: row.name
            for row in con.execute(
                select(tags.c.id, tags.c.name)
                .where(tags.c.id.in_(tag_ids))
            )
        }

        # Lấy dữ liệu tag_values trong khoảng thời gian
        stmt = select(
            tag_values.c.tag_id,
            tag_values.c.ts,
            tag_values.c.value
        ).where(tag_values.c.tag_id.in_(tag_ids))

        if dt_from:
            stmt = stmt.where(tag_values.c.ts >= dt_from)
        if dt_to:
            stmt = stmt.where(tag_values.c.ts <= dt_to)

        stmt = stmt.order_by(asc(tag_values.c.ts))
        rows = con.execute(stmt).fetchall()
        # print(f"Fetched {len(rows)} rows for logger {logger_id} from {dt_from} to {dt_to}")
        # Gom dữ liệu theo timestamp
        data_map = {}
        for tag_id, ts, value in rows:
            ts_str = ts.strftime("%d/%m/%Y %H:%M:%S")
            if ts_str not in data_map:
                data_map[ts_str] = {}
            data_map[ts_str][tags_map[tag_id]] = value

        # Chuyển thành list dict và danh sách cột
        columns = ["timestamp"] + [tags_map[tid] for tid in tag_ids]
        items = []
        for ts_str in sorted(data_map.keys()):
            row_dict = {"timestamp": ts_str}
            for tag_name in columns[1:]:
                row_dict[tag_name] = data_map[ts_str].get(tag_name)
            items.append(row_dict)

        return items, columns

def get_all_logger_rows(dt_from, dt_to):
    items_all = []
    columns = ["timestamp"]
    with init_engine().connect() as con:
        loggers = con.execute(select(data_loggers.c.id, data_loggers.c.name)).mappings().all()
        for lg in loggers:
            lg_items, lg_cols = get_logger_rows(lg["id"], dt_from, dt_to)
            # đổi tên cột tag -> "LoggerName.TagName"
            for col in lg_cols:
                if col == "timestamp": continue
                new_col = f"{lg['name']}.{col}"
                columns.append(new_col)
                for it in lg_items:
                    ts = it["timestamp"]
                    row = next((x for x in items_all if x["timestamp"] == ts), None)
                    if not row:
                        row = {"timestamp": ts}
                        items_all.append(row)
                    row[new_col] = it.get(col)
    return items_all, columns


def get_latest_tag_value(tag_id: int):
    """
    Ultra-fast version sử dụng bảng tag_latest_values với format giá trị theo datatype
    """
    with init_engine().connect() as con:
        # Join với bảng tags để lấy datatype
        row = con.execute(
            select(
                tag_latest_values.c.value, 
                tag_latest_values.c.ts,
                tags.c.datatype
            )
            .select_from(tag_latest_values.join(tags, tag_latest_values.c.tag_id == tags.c.id))
            .where(tag_latest_values.c.tag_id == tag_id)
        ).first()
        
        if not row:
            return (None, None)
            
        value, ts, datatype = row
        
        # Format giá trị theo datatype
        if datatype in ["Word", "Short", "DWord", "DInt", "Bit", "Signed", "Unsigned", "Long", "Long_inverse", "Hex", "Binary"]:
            # Các kiểu số nguyên - loại bỏ .0, hỗ trợ số âm
            try:
                if float(value).is_integer():
                    formatted_value = int(value)
                else:
                    formatted_value = value
            except (ValueError, TypeError):
                formatted_value = value
        else:
            # Float, Double, Binary, Hex, Raw và các kiểu khác - giữ nguyên
            formatted_value = value
        return (formatted_value, ts)

def get_latest_tag_values_batch(tag_ids: List[int]) -> dict:
    """
    Lấy latest values cho nhiều tags cùng lúc từ bảng tag_latest_values với format giá trị theo datatype
    Returns: {tag_id: (value, timestamp), ...}
    """
    if not tag_ids:
        return {}
    
    result = {}
    
    with init_engine().connect() as con:
        # Join với bảng tags để lấy datatype
        rows = con.execute(
            select(
                tag_latest_values.c.tag_id,
                tag_latest_values.c.value,
                tag_latest_values.c.ts,
                tags.c.datatype
            )
            .select_from(tag_latest_values.join(tags, tag_latest_values.c.tag_id == tags.c.id))
            .where(tag_latest_values.c.tag_id.in_(tag_ids))
        ).mappings().all()
        
        # Populate result với formatted data từ database
        for row in rows:
            value = row['value']
            datatype = row['datatype']
            
            # Format giá trị theo datatype
            if datatype in ["Word", "Short", "DWord", "DInt", "Bit", "Signed", "Unsigned", "Long", "Long_inverse", "Hex", "Binary"]:
                # Các kiểu số nguyên - loại bỏ .0, hỗ trợ số âm
                try:
                    if float(value).is_integer():
                        formatted_value = int(value)
                    else:
                        formatted_value = value
                except (ValueError, TypeError):
                    formatted_value = value
            else:
                # Float, Double, Binary, Hex, Raw và các kiểu khác - giữ nguyên
                formatted_value = value
                
            result[row['tag_id']] = (formatted_value, row['ts'])
        
        # Đối với các tag không có data, set None
        for tag_id in tag_ids:
            if tag_id not in result:
                result[tag_id] = (None, None)
    
    return result
    
def get_tag_logger_map(device_id: int):
    """
    Trả về dict: {tag_id: {"interval_sec": ..., "logger_id": ...}, ...}
    """
    with init_engine().connect() as con:
        stmt = (
            select(
                tags.c.id.label("tag_id"),
                data_loggers.c.interval_sec,
                data_loggers.c.id.label("logger_id")
            )
            .select_from(tags
                .join(data_logger_tags, data_logger_tags.c.tag_id == tags.c.id)
                .join(data_loggers, data_loggers.c.id == data_logger_tags.c.logger_id)
            )
            .where(tags.c.device_id == device_id, data_loggers.c.enabled == 1)
        )

        rows = con.execute(stmt).mappings().all() 

    return {row["tag_id"]: dict(row) for row in rows}

### USER
def list_users():
    with init_engine().connect() as con:
        rows = con.execute(select(users)).mappings().all()
        return [dict(r) for r in rows]

def get_user_by_username(username: str):
    with init_engine().connect() as con:
        r = con.execute(select(users).where(users.c.username == username)).mappings().first()
        return dict(r) if r else None

def add_user_row(data: dict) -> int:
    with init_engine().begin() as con:
        res = con.execute(insert(users).values(**data))
        return res.inserted_primary_key[0]

def update_user_row(user_id: int, data: dict) -> int:
    data = {k: v for k, v in data.items() if k in users.c and v is not None}
    with init_engine().begin() as con:
        res = con.execute(update(users).where(users.c.id == user_id).values(**data))
        return res.rowcount

def delete_user_row(user_id: int) -> int:
    with init_engine().connect() as con:
        # Check the user's role
        user = con.execute(select(users).where(users.c.id == user_id)).mappings().first()
        if user and user["role"] == "admin":
            raise ValueError("Cannot delete admin users.")
    
    with init_engine().begin() as con:
        res = con.execute(delete(users).where(users.c.id == user_id))
        return res.rowcount

def get_notification_recipients():
    """Get all users with notification enabled and valid email/phone"""
    with init_engine().connect() as con:
        # Get users with notification enabled and have email or phone
        stmt = select(users).where(
            and_(
                users.c.notification_enabled == True,
                func.coalesce(users.c.email, users.c.phone).is_not(None)
            )
        )
        
        rows = con.execute(stmt).mappings().all()
        recipients = []
        
        for row in rows:
            user = dict(row)
            # Only include users that have either email or phone
            if user.get('email') or user.get('phone'):
                recipients.append(user)
        
        return recipients
        res = con.execute(delete(users).where(users.c.id == user_id))
        return res.rowcount

### Subdashboard
def list_subdashboards():
    """Return all subdashboards (dashboards table)."""
    with init_engine().connect() as con:
        rows = con.execute(select(dashboards).order_by(dashboards.c.id.asc())).mappings().all()
        return [dict(r) for r in rows]

def add_tag_to_subdashboard(sid: int, tag_id: int):
    with init_engine().begin() as con:
        exists = con.execute(
            select(dashboard_tags)
            .where(
                dashboard_tags.c.dashboard_id == sid,
                dashboard_tags.c.tag_id == tag_id
            )
        ).first()
        if not exists:
            con.execute(
                dashboard_tags.insert().values(dashboard_id=sid, tag_id=tag_id)
            )

def get_subdashboard(sid: int):
    """Get subdashboard by id"""
    with init_engine().connect() as con:
        r = con.execute(select(dashboards).where(dashboards.c.id == sid)).mappings().first()
        return dict(r) if r else None
        
def add_subdashboard_row(data: dict, tag_ids: List[int] = None) -> int:
    """Add a new subdashboard and optionally attach tags."""
    with init_engine().begin() as con:
        res = con.execute(insert(dashboards).values(**data))
        new_id = res.inserted_primary_key[0]
        if tag_ids:
            con.execute(
                dashboard_tags.insert(),
                [{"dashboard_id": new_id, "tag_id": tid} for tid in tag_ids]
            )
        return new_id

def delete_subdashboard_row(sid: int) -> int:
    """Delete a subdashboard and its tag mappings."""
    with init_engine().begin() as con:
        # ON DELETE CASCADE will remove dashboard_tags
        res = con.execute(delete(dashboards).where(dashboards.c.id == sid))
        return res.rowcount

def update_subdashboard_row(sid: int, data: dict) -> int:
    """Update a subdashboard."""
    with init_engine().begin() as con:
        res = con.execute(update(dashboards).where(dashboards.c.id == sid).values(**data))
        return res.rowcount
    
def get_subdashboard_tags(sid: int):
    """
    Trả về danh sách tag (dict) thuộc subdashboard (dashboard) có id=sid.
    Lấy cả giá trị latest và timestamp.
    """
    with init_engine().connect() as con:
        rows = con.execute(
            select(
                tags.c.id,
                tags.c.name,
                tags.c.description,
                tags.c.datatype,
                tags.c.unit,
                tags.c.device_id,
                tags.c.function_code,
                tags.c.address,
                tags.c.scale,
                tags.c.offset
            )
            .select_from(
                dashboard_tags.join(tags, dashboard_tags.c.tag_id == tags.c.id)
            )
            .where(dashboard_tags.c.dashboard_id == sid)
        ).mappings().all()
        
        result = []
        for r in rows:
            tag_dict = dict(r)
            # Get latest value and timestamp for each tag
            value, ts = get_latest_tag_value(tag_dict['id'])
            tag_dict['value'] = value
            tag_dict['ts'] = ts.strftime("%H:%M:%S") if ts else "--:--"
            tag_dict['alarm_status'] = "Normal"  # You can add alarm logic here
            result.append(tag_dict)
        
        return result
    
def list_subdash_groups():
    with init_engine().connect() as con:
        return con.execute(select(subdash_tag_groups)).mappings().all()

def list_subdash_groups_for_dashboard(dashboard_id: int):
    """Get all groups for a specific subdashboard."""
    with init_engine().connect() as con:
        try:
            # Preferred: order by creation time (oldest first)
            return con.execute(
                select(subdash_tag_groups)
                .where(subdash_tag_groups.c.dashboard_id == dashboard_id)
                .order_by(subdash_tag_groups.c.created_at.asc(), subdash_tag_groups.c.id.asc())
            ).mappings().all()
        except Exception:
            # Fallback: legacy ordering (by 'order' then name) if column not available
            return con.execute(
                select(subdash_tag_groups)
                .where(subdash_tag_groups.c.dashboard_id == dashboard_id)
                .order_by(subdash_tag_groups.c.order.asc(), subdash_tag_groups.c.name.asc())
            ).mappings().all()

def add_subdash_group(data: dict):
    with init_engine().begin() as con:
        res = con.execute(insert(subdash_tag_groups).values(**data))
        return res.inserted_primary_key[0]

def get_subdash_group(gid: int):
    with init_engine().connect() as con:
        return con.execute(select(subdash_tag_groups).where(subdash_tag_groups.c.id == gid)).mappings().first()

def update_subdash_group(gid: int, data: dict):
    with init_engine().begin() as con:
        con.execute(update(subdash_tag_groups).where(subdash_tag_groups.c.id == gid).values(**data))

def delete_subdash_group(gid: int):
    with init_engine().begin() as con:
        # First delete all tag associations
        result1 = con.execute(delete(subdash_group_tags).where(subdash_group_tags.c.group_id == gid))
        print(f"Deleted {result1.rowcount} tag associations for group {gid}")
        
        # Then delete the group itself
        result2 = con.execute(delete(subdash_tag_groups).where(subdash_tag_groups.c.id == gid))
        print(f"Deleted group {gid}, affected rows: {result2.rowcount}")

def add_tag_to_subdash_group(group_id: int, tag_id: int):
    """Add a tag to a subdashboard group"""
    with init_engine().begin() as con:
        # Check if the relationship already exists
        exists = con.execute(
            select(subdash_group_tags)
            .where(
                subdash_group_tags.c.group_id == group_id,
                subdash_group_tags.c.tag_id == tag_id
            )
        ).first()
        
        if not exists:
            con.execute(
                subdash_group_tags.insert().values(group_id=group_id, tag_id=tag_id)
            )

def remove_tag_from_subdash_group(group_id: int, tag_id: int):
    """Remove a tag from a subdashboard group"""
    with init_engine().begin() as con:
        result = con.execute(
            subdash_group_tags.delete().where(
                subdash_group_tags.c.group_id == group_id,
                subdash_group_tags.c.tag_id == tag_id
            )
        )
        print(f"Removed tag {tag_id} from group {group_id}, affected rows: {result.rowcount}")

def get_tags_of_group(group_id: int):
    """Get all tags with full details for a specific group."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(
                tags.c.id,
                tags.c.name,
                tags.c.description,
                tags.c.unit,
                tags.c.datatype,
                tags.c.function_code,
                tags.c.device_id
            )
            .select_from(
                subdash_group_tags.join(tags, subdash_group_tags.c.tag_id == tags.c.id)
            )
            .where(subdash_group_tags.c.group_id == group_id)
            .order_by(subdash_group_tags.c.created_at.asc(), subdash_group_tags.c.tag_id.asc())
        ).mappings().all()
        
        result = []
        for r in rows:
            tag_dict = dict(r)
            # Get latest value and timestamp for each tag
            value, ts = get_latest_tag_value(tag_dict['id'])
            # print(f"Tag {tag_dict['name']} latest value: {value} at {ts}")
            tag_dict['value'] = value
            tag_dict['ts'] = ts.strftime("%H:%M:%S") if ts else "--:--"
            tag_dict['alarm_status'] = "Normal"  # You can add alarm logic here
            result.append(tag_dict)
        
        return result

def get_recent_alarm_events(since: datetime = None):
    """Get recent alarm events for notification system"""
    if since is None:
        since = safe_datetime_now() - timedelta(hours=1)
    
    with init_engine().connect() as con:
        # Join alarm_events với tags để lấy tag name
        query = select(
            alarm_events.c.id,
            alarm_events.c.ts.label("created_at"),
            alarm_events.c.name.label("alarm_name"),
            alarm_events.c.level,
            alarm_events.c.target.label("tag_id"),
            alarm_events.c.value,
            alarm_events.c.note.label("message"),
            alarm_events.c.event_type,
            tags.c.name.label("tag_name")
        ).select_from(
            alarm_events.join(tags, alarm_events.c.target == tags.c.id)
        ).where(
            alarm_events.c.ts >= since
        ).order_by(alarm_events.c.ts.desc())
        
        rows = con.execute(query).mappings().all()
        return [dict(r) for r in rows]

def update_tag_unit(tag_id: int, unit: str) -> bool:
    """Update unit for a specific tag"""
    try:
        with init_engine().connect() as con:
            result = con.execute(
                update(tags)
                .where(tags.c.id == tag_id)
                .values(unit=unit)
            )
            con.commit()
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating tag unit: {e}")
        return False

# ----------- DATA LOGGERS -----------
def list_data_loggers():
    """Get all data loggers"""
    try:
        with init_engine().connect() as con:
            rows = con.execute(select(data_loggers).order_by(data_loggers.c.id.asc())).mappings().all()
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error listing data loggers: {e}")
        return []

def get_logger_tag_ids(logger_id: int) -> List[int]:
    """Get tag IDs associated with a logger"""
    try:
        with init_engine().connect() as con:
            rows = con.execute(
                select(data_logger_tags.c.tag_id)
                .where(data_logger_tags.c.logger_id == logger_id)
            ).fetchall()
            return [row[0] for row in rows]
    except Exception as e:
        print(f"Error getting logger tag IDs: {e}")
        return []

def get_tag_logger_map(device_id: int = None) -> dict:
    """Get mapping of tag_id -> logger info for a device"""
    try:
        with init_engine().connect() as con:
            # Query to get tag_id -> logger mapping
            if device_id:
                # Filter by device if specified
                query = select(
                    data_logger_tags.c.tag_id,
                    data_loggers.c.id.label("logger_id"),
                    data_loggers.c.name.label("logger_name"),
                    data_loggers.c.interval_sec,
                    data_loggers.c.enabled
                ).select_from(
                    data_logger_tags
                    .join(data_loggers, data_logger_tags.c.logger_id == data_loggers.c.id)
                    .join(tags, data_logger_tags.c.tag_id == tags.c.id)
                ).where(tags.c.device_id == device_id)
            else:
                # Get all tag logger mappings
                query = select(
                    data_logger_tags.c.tag_id,
                    data_loggers.c.id.label("logger_id"),
                    data_loggers.c.name.label("logger_name"),
                    data_loggers.c.interval_sec,
                    data_loggers.c.enabled
                ).select_from(
                    data_logger_tags.join(data_loggers, data_logger_tags.c.logger_id == data_loggers.c.id)
                )
            
            rows = con.execute(query).mappings().all()
            
            result = {}
            for row in rows:
                result[row["tag_id"]] = {
                    "logger_id": row["logger_id"],
                    "logger_name": row["logger_name"],
                    "interval_sec": row["interval_sec"],
                    "enabled": row["enabled"]
                }
            
            return result
    except Exception as e:
        print(f"Error getting tag logger map: {e}")
        return {}

def batch_insert_data_logs(log_entries: List[dict]):
    """Batch insert data log entries into tag_values table"""
    try:
        if not log_entries:
            return
            
        # Convert log entries to tag_values format
        tag_value_entries = []
        current_time = safe_datetime_now()
        
        for entry in log_entries:
            tag_value_entries.append({
                'tag_id': entry['tag_id'],
                'ts': datetime.fromtimestamp(entry['timestamp']) if isinstance(entry['timestamp'], (int, float)) else entry['timestamp'],
                'value': entry['value']
            })
        
        with init_engine().begin() as con:
            # Batch insert into tag_values table
            con.execute(tag_values.insert(), tag_value_entries)
            
            # Also update tag_latest_values for quick access
            for entry in tag_value_entries:
                con.execute(
                    text("""
                        INSERT INTO tag_latest_values (tag_id, value, ts, updated_at)
                        VALUES (:tag_id, :value, :ts, :updated_at)
                        ON DUPLICATE KEY UPDATE
                        value = VALUES(value),
                        ts = VALUES(ts),
                        updated_at = VALUES(updated_at)
                    """),
                    {
                        'tag_id': entry['tag_id'],
                        'value': entry['value'],
                        'ts': entry['ts'],
                        'updated_at': current_time
                    }
                )
            
        print(f"Batch inserted {len(tag_value_entries)} data log entries")
        
    except Exception as e:
        print(f"Error batch inserting data logs: {e}")
        raise

def add_data_logger(name: str, interval_sec: float, enabled: bool = True, description: str = "") -> int:
    """Add a new data logger"""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                insert(data_loggers).values(
                    name=name,
                    interval_sec=interval_sec,
                    enabled=enabled,
                    description=description
                )
            )
            return result.inserted_primary_key[0]
    except Exception as e:
        print(f"Error adding data logger: {e}")
        raise

def update_data_logger(logger_id: int, data: dict) -> int:
    """Update data logger"""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                update(data_loggers)
                .where(data_loggers.c.id == logger_id)
                .values(**data)
            )
            return result.rowcount
    except Exception as e:
        print(f"Error updating data logger: {e}")
        raise

def delete_data_logger(logger_id: int) -> int:
    """Delete data logger and its tag associations"""
    try:
        with init_engine().begin() as con:
            # Delete tag associations first (CASCADE should handle this, but explicit is better)
            con.execute(
                delete(data_logger_tags).where(data_logger_tags.c.logger_id == logger_id)
            )
            
            # Delete the logger
            result = con.execute(
                delete(data_loggers).where(data_loggers.c.id == logger_id)
            )
            return result.rowcount
    except Exception as e:
        print(f"Error deleting data logger: {e}")
        raise

def assign_tag_to_logger(logger_id: int, tag_id: int):
    """Assign a tag to a data logger"""
    try:
        with init_engine().begin() as con:
            # Check if already exists
            exists = con.execute(
                select(data_logger_tags)
                .where(
                    data_logger_tags.c.logger_id == logger_id,
                    data_logger_tags.c.tag_id == tag_id
                )
            ).first()
            
            if not exists:
                con.execute(
                    data_logger_tags.insert().values(
                        logger_id=logger_id,
                        tag_id=tag_id
                    )
                )
    except Exception as e:
        print(f"Error assigning tag to logger: {e}")
        raise

def remove_tag_from_logger(logger_id: int, tag_id: int):
    """Remove a tag from a data logger"""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(data_logger_tags).where(
                    data_logger_tags.c.logger_id == logger_id,
                    data_logger_tags.c.tag_id == tag_id
                )
            )
            return result.rowcount
    except Exception as e:
        print(f"Error removing tag from logger: {e}")
        raise

def update_tag_latest_value(tag_id: int, value: float, timestamp: datetime):
    """
    Update or insert latest value for a single tag.
    This ensures ALL tags get their latest values saved, not just datalogger tags.
    """
    try:
        with init_engine().begin() as con:
            # Use MySQL's ON DUPLICATE KEY UPDATE for efficient upsert
            con.execute(
                text("""
                    INSERT INTO tag_latest_values (tag_id, value, ts, updated_at)
                    VALUES (:tag_id, :value, :ts, :updated_at)
                    ON DUPLICATE KEY UPDATE
                    value = VALUES(value),
                    ts = VALUES(ts),
                    updated_at = VALUES(updated_at)
                """),
                {
                    'tag_id': tag_id,
                    'value': value,
                    'ts': timestamp,
                    'updated_at': safe_datetime_now()
                }
            )
    except Exception as e:
        print(f"Error updating latest value for tag {tag_id}: {e}")
        raise

# ----------- WORKER MANAGEMENT -----------

def list_workers():
    """Get all worker configurations"""
    try:
        with init_engine().connect() as con:
            rows = con.execute(select(workers).order_by(workers.c.worker_id.asc())).mappings().all()
            result = []
            for row in rows:
                worker = dict(row)
                # Get assigned devices
                device_rows = con.execute(
                    select(worker_devices.c.device_id)
                    .where(worker_devices.c.worker_id == worker['worker_id'])
                ).mappings().all()
                worker['device_ids'] = [r['device_id'] for r in device_rows]
                result.append(worker)
            return result
    except Exception as e:
        print(f"Error listing workers: {e}")
        return []

def get_worker(worker_id: str):
    """Get worker configuration by ID"""
    try:
        with init_engine().connect() as con:
            row = con.execute(
                select(workers).where(workers.c.worker_id == worker_id)
            ).mappings().first()
            
            if not row:
                return None
                
            worker = dict(row)
            # Get assigned devices
            device_rows = con.execute(
                select(worker_devices.c.device_id)
                .where(worker_devices.c.worker_id == worker_id)
            ).mappings().all()
            worker['device_ids'] = [r['device_id'] for r in device_rows]
            return worker
    except Exception as e:
        print(f"Error getting worker {worker_id}: {e}")
        return None

def add_worker(worker_data: dict, device_ids: list = None) -> str:
    """Add a new worker configuration"""
    try:
        with init_engine().begin() as con:
            # Insert worker
            result = con.execute(insert(workers).values(**worker_data))
            worker_id = worker_data['worker_id']
            
            # Assign devices
            if device_ids:
                device_assignments = [
                    {"worker_id": worker_id, "device_id": device_id}
                    for device_id in device_ids
                ]
                con.execute(insert(worker_devices), device_assignments)
            
            return worker_id
    except Exception as e:
        print(f"Error adding worker: {e}")
        raise

def update_worker(worker_id: str, worker_data: dict, device_ids: list = None) -> bool:
    """Update worker configuration"""
    try:
        with init_engine().begin() as con:
            # Update worker
            result = con.execute(
                update(workers)
                .where(workers.c.worker_id == worker_id)
                .values(**worker_data)
            )
            
            # Update device assignments if provided
            if device_ids is not None:
                # Remove existing assignments
                con.execute(
                    delete(worker_devices).where(worker_devices.c.worker_id == worker_id)
                )
                
                # Add new assignments
                if device_ids:
                    device_assignments = [
                        {"worker_id": worker_id, "device_id": device_id}
                        for device_id in device_ids
                    ]
                    con.execute(insert(worker_devices), device_assignments)
            
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating worker {worker_id}: {e}")
        return False

def delete_worker(worker_id: str) -> bool:
    """Delete worker configuration"""
    try:
        with init_engine().begin() as con:
            # Delete worker (assignments will be cascade deleted)
            result = con.execute(
                delete(workers).where(workers.c.worker_id == worker_id)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting worker {worker_id}: {e}")
        return False

def get_worker_devices(worker_id: str):
    """Get devices assigned to a worker with full device info"""
    try:
        with init_engine().connect() as con:
            rows = con.execute(
                select(devices)
                .select_from(
                    devices.join(
                        worker_devices, 
                        devices.c.id == worker_devices.c.device_id
                    )
                )
                .where(worker_devices.c.worker_id == worker_id)
            ).mappings().all()
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error getting worker devices: {e}")
        return []

def get_auto_start_workers():
    """
    Get workers configuration based on devices table.
    Group devices by connection type (TCP host:port or RTU serial_port:baudrate)
    to create worker configurations automatically.
    """
    try:
        with init_engine().connect() as con:
            # Get all devices (không lọc theo is_online vì đó là trạng thái động)
            device_rows = con.execute(
                select(devices)
                .order_by(devices.c.protocol.asc(), devices.c.host.asc(), devices.c.serial_port.asc())
            ).mappings().all()
            
            if not device_rows:
                return []
            
            # Group devices by connection parameters to create workers
            tcp_groups = {}  # {(host, port): [devices]}
            rtu_groups = {}  # {(serial_port, baudrate): [devices]}
            
            for device_row in device_rows:
                device = dict(device_row)
                
                if device['protocol'] == 'ModbusTCP' and device['host'] and device['port']:
                    # TCP worker - group by host:port
                    key = (device['host'], device['port'])
                    if key not in tcp_groups:
                        tcp_groups[key] = []
                    tcp_groups[key].append(device)
                    
                elif device['protocol'] == 'ModbusRTU' and device['serial_port'] and device['baudrate']:
                    # RTU worker - group by serial_port:baudrate
                    key = (device['serial_port'], device['baudrate'])
                    if key not in rtu_groups:
                        rtu_groups[key] = []
                    rtu_groups[key].append(device)
            
            result = []
            
            # Create TCP workers
            for (host, port), devices_list in tcp_groups.items():
                # Get all tags for all devices in this group
                all_tags = []
                for device in devices_list:
                    device_tags = list_tags(device['id'])
                    all_tags.extend(device_tags)
                
                # Tạo worker ngay cả khi chưa có tags (có thể thêm tags sau)
                worker_config = {
                    'worker_id': f'tcp_{host}_{port}'.replace('.', '_'),
                    'worker_type': 'tcp',
                    'enabled': True,
                    'auto_start': True,
                    'host': host,
                    'port': port,
                    'polling_interval': 1.0,
                    'description': f'Auto-generated TCP worker for {host}:{port}',
                    'devices': devices_list,
                    'tags': all_tags
                }
                result.append(worker_config)
            
            # Create RTU workers  
            for (serial_port, baudrate), devices_list in rtu_groups.items():
                # Get all tags for all devices in this group
                all_tags = []
                for device in devices_list:
                    device_tags = list_tags(device['id'])
                    all_tags.extend(device_tags)
                
                # Tạo worker ngay cả khi chưa có tags (có thể thêm tags sau)
                worker_config = {
                    'worker_id': f'rtu_{serial_port}_{baudrate}'.replace('/', '_').replace('\\', '_').replace(':', '_'),
                    'worker_type': 'rtu',
                    'enabled': True,
                    'auto_start': True,
                    'serial_port': serial_port,
                    'baudrate': baudrate,
                    'polling_interval': 1.0,
                    'description': f'Auto-generated RTU worker for {serial_port}@{baudrate}',
                    'devices': devices_list,
                    'tags': all_tags
                }
                result.append(worker_config)
            
            return result
            
    except Exception as e:
        print(f"Error getting auto-start workers: {e}")
        return []

# ----------- TAG LOGS (DATALOGGER) -----------

def bulk_insert_tag_logs(log_entries: List[tuple]) -> int:
    """
    Bulk insert/upsert tag logs with ON DUPLICATE KEY UPDATE for idempotency
    log_entries = [(logger_id, tag_id, ts, value), ...]
    Returns: number of rows inserted/updated
    """
    if not log_entries:
        return 0
    
    with init_engine().begin() as con:
        try:
            # Convert tuples to list of parameter dictionaries for raw SQL
            params_list = []
            for entry in log_entries:
                if len(entry) == 4:
                    logger_id, tag_id, ts, value = entry
                    params_list.append({
                        'logger_id': int(logger_id),
                        'tag_id': int(tag_id), 
                        'ts': ts,
                        'value': float(value)
                    })
            
            if not params_list:
                return 0
            
            # Use executemany for bulk insert with ON DUPLICATE KEY UPDATE
            sql = """
            INSERT INTO tag_logs (logger_id, tag_id, ts, value) 
            VALUES (:logger_id, :tag_id, :ts, :value)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value),
                created_at = CURRENT_TIMESTAMP
            """
            
            # Execute each row separately to handle MySQL syntax properly
            total_affected = 0
            for params in params_list:
                result = con.execute(text(sql), params)
                total_affected += result.rowcount if result.rowcount else 1
            
            return total_affected
            return total_affected  # Return number of rows processed
            
        except Exception as e:
            print(f"Error in bulk_insert_tag_logs: {e}")
            import traceback
            traceback.print_exc()
            raise

def purge_tag_logs_by_logger(logger_id: int, dt_from: datetime = None, dt_to: datetime = None) -> int:
    """
    Delete tag logs for a specific logger within optional date range
    Returns: number of deleted rows
    """
    with init_engine().begin() as con:
        conditions = [tag_logs.c.logger_id == logger_id]
        
        if dt_from:
            conditions.append(tag_logs.c.ts >= dt_from)
        if dt_to:
            conditions.append(tag_logs.c.ts <= dt_to)
        
        stmt = delete(tag_logs).where(and_(*conditions))
        result = con.execute(stmt)
        return result.rowcount

def purge_tag_logs_by_timerange(dt_from: datetime, dt_to: datetime) -> int:
    """
    Delete all tag logs within date range
    Returns: number of deleted rows
    """
    with init_engine().begin() as con:
        stmt = delete(tag_logs).where(
            and_(tag_logs.c.ts >= dt_from, tag_logs.c.ts <= dt_to)
        )
        result = con.execute(stmt)
        return result.rowcount

def get_tag_logs_count(logger_id: int = None, dt_from: datetime = None, dt_to: datetime = None) -> int:
    """
    Get count of tag logs with optional filters
    """
    with init_engine().connect() as con:
        conditions = []
        
        if logger_id:
            conditions.append(tag_logs.c.logger_id == logger_id)
        if dt_from:
            conditions.append(tag_logs.c.ts >= dt_from)
        if dt_to:
            conditions.append(tag_logs.c.ts <= dt_to)
        
        if conditions:
            stmt = select(func.count(tag_logs.c.id)).where(and_(*conditions))
        else:
            stmt = select(func.count(tag_logs.c.id))
        
        result = con.execute(stmt)
        return result.scalar() or 0

def get_tag_logs_for_logger(logger_id: int, dt_from: datetime = None, dt_to: datetime = None, limit: int = 1000):
    """
    Get tag logs for a specific logger with optional filtering
    """
    with init_engine().connect() as con:
        conditions = [tag_logs.c.logger_id == logger_id]
        
        if dt_from:
            conditions.append(tag_logs.c.ts >= dt_from)
        if dt_to:
            conditions.append(tag_logs.c.ts <= dt_to)
        
        stmt = (select(tag_logs)
                .where(and_(*conditions))
                .order_by(tag_logs.c.ts.desc())
                .limit(limit))
        
        rows = con.execute(stmt).mappings().all()
        return [dict(row) for row in rows]

def get_logger_statistics(logger_id: int) -> dict:
    """
    Get statistics for a specific logger
    """
    with init_engine().connect() as con:
        # Total count
        total_stmt = select(func.count(tag_logs.c.id)).where(tag_logs.c.logger_id == logger_id)
        total_count = con.execute(total_stmt).scalar() or 0
        
        # Date range
        range_stmt = select(
            func.min(tag_logs.c.ts).label('oldest'),
            func.max(tag_logs.c.ts).label('newest'),
            func.count(func.distinct(tag_logs.c.tag_id)).label('unique_tags')
        ).where(tag_logs.c.logger_id == logger_id)
        
        range_result = con.execute(range_stmt).mappings().first()
        
        return {
            'logger_id': logger_id,
            'total_records': total_count,
            'oldest_record': range_result['oldest'] if range_result else None,
            'newest_record': range_result['newest'] if range_result else None,
            'unique_tags': range_result['unique_tags'] if range_result else 0
        }

def get_datalogger_data(logger_id: int, dt_from: datetime, dt_to: datetime, offset: int = 0, limit: int = None, count_only: bool = False):
    """
    Load dữ liệu datalogger từ bảng tag_logs cho một logger cụ thể
    Returns: (items, columns, total_count) - items là list dict, columns là list tên cột, total_count là tổng số rows
    """
    with init_engine().connect() as con:
        # Lấy thông tin logger và tags
        logger_info = con.execute(
            select(data_loggers.c.name).where(data_loggers.c.id == logger_id)
        ).mappings().first()
        
        if not logger_info:
            return [], ["timestamp"], 0
        
        # Lấy tags được assign cho logger này
        tag_info = con.execute(
            select(
                tags.c.id,
                tags.c.name,
                tags.c.datatype,
                data_logger_tags.c.created_at.label("attached_at")
            )
            .select_from(
                tags.join(data_logger_tags, tags.c.id == data_logger_tags.c.tag_id)
            )
            .where(data_logger_tags.c.logger_id == logger_id)
            # Thứ tự theo thời điểm tag được gắn vào logger (tag mới thêm sẽ xếp sau)
            .order_by(data_logger_tags.c.created_at.asc(), tags.c.id.asc())
        ).mappings().all()
        
        if not tag_info:
            return [], ["timestamp"], 0
        
        # Load data từ tag_logs
        log_data = con.execute(
            select(
                tag_logs.c.tag_id,
                tag_logs.c.ts,
                tag_logs.c.value
            )
            .where(
                and_(
                    tag_logs.c.logger_id == logger_id,
                    tag_logs.c.ts.between(dt_from, dt_to)
                )
            )
            .order_by(tag_logs.c.ts, tag_logs.c.tag_id)
        ).mappings().all()
        
        # Tạo columns: timestamp + tag names
        tag_id_to_name = {tag['id']: tag['name'] for tag in tag_info}
        columns = ["timestamp"] + [tag['name'] for tag in tag_info]
        
        # Group data theo timestamp
        timestamp_data = {}
        for row in log_data:
            # Skip tags không còn được assign cho logger này (có thể đã bị xóa/unassign)
            if row['tag_id'] not in tag_id_to_name:
                continue
            
            ts_str = row['ts'].strftime("%d/%m/%Y %H:%M:%S")
            if ts_str not in timestamp_data:
                timestamp_data[ts_str] = {}
            timestamp_data[ts_str][tag_id_to_name[row['tag_id']]] = row['value']
        
        # Convert sang format cho template
        items = []
        for ts_str in sorted(timestamp_data.keys()):  # Oldest first
            row = {"timestamp": ts_str}
            for tag in tag_info:
                tag_name = tag['name']
                row[tag_name] = timestamp_data[ts_str].get(tag_name, None)
            items.append(row)
        
        # Total count trước khi pagination
        total_count = len(items)
        
        # Apply pagination
        if offset > 0:
            items = items[offset:]
        if limit is not None:
            items = items[:limit]
        
        return items, columns, total_count

def get_all_datalogger_data(dt_from: datetime, dt_to: datetime, offset: int = 0, limit: int = None):
    """
    Load dữ liệu cho tất cả dataloggers từ bảng tag_logs
    Returns: (items, columns, total_count)
    """
    with init_engine().connect() as con:
        # Load tất cả data trong khoảng thời gian từ dataloggers có data
        log_data = con.execute(
            select(
                data_loggers.c.name.label('logger_name'),
                tags.c.name.label('tag_name'),
                tag_logs.c.ts,
                tag_logs.c.value
            )
            .select_from(
                tag_logs
                .join(data_loggers, tag_logs.c.logger_id == data_loggers.c.id)
                .join(tags, tag_logs.c.tag_id == tags.c.id)
            )
            .where(tag_logs.c.ts.between(dt_from, dt_to))
            .order_by(tag_logs.c.ts, data_loggers.c.name, tags.c.name)
        ).mappings().all()
        
        if not log_data:
            return [], ["timestamp"], 0
        
        # Tạo columns theo thứ tự gắn tag vào logger (created_at) để tag mới thêm xếp sau
        # 1) Xác định các cột thực sự có dữ liệu trong khoảng thời gian lọc
        have_cols = set()
        for row in log_data:
            have_cols.add(f"{row['logger_name']}.{row['tag_name']}")

        # 2) Lấy thứ tự theo logger_name + data_logger_tags.created_at
        tag_orders = con.execute(
            select(
                data_loggers.c.name.label('logger_name'),
                tags.c.name.label('tag_name'),
                data_logger_tags.c.created_at
            )
            .select_from(
                data_logger_tags
                .join(data_loggers, data_logger_tags.c.logger_id == data_loggers.c.id)
                .join(tags, data_logger_tags.c.tag_id == tags.c.id)
            )
            .order_by(data_loggers.c.name.asc(), data_logger_tags.c.created_at.asc(), tags.c.id.asc())
        ).mappings().all()

        ordered_cols = []
        seen = set()
        for r in tag_orders:
            col = f"{r['logger_name']}.{r['tag_name']}"
            if col in have_cols and col not in seen:
                seen.add(col)
                ordered_cols.append(col)

        columns = ["timestamp"] + ordered_cols
        
        # Group data theo timestamp
        timestamp_data = {}
        for row in log_data:
            ts_str = row['ts'].strftime("%d/%m/%Y %H:%M:%S")
            if ts_str not in timestamp_data:
                timestamp_data[ts_str] = {}
            col_name = f"{row['logger_name']}.{row['tag_name']}"
            timestamp_data[ts_str][col_name] = row['value']
        
        # Convert sang format cho template
        items = []
        for ts_str in sorted(timestamp_data.keys()):  # Oldest first
            row = {"timestamp": ts_str}
            for col in columns[1:]:  # Skip timestamp column
                row[col] = timestamp_data[ts_str].get(col, None)
            items.append(row)
        
        # Total count trước khi pagination
        total_count = len(items)
        
        # Apply pagination
        if offset > 0:
            items = items[offset:]
        if limit is not None:
            items = items[:limit]
        
        return items, columns, total_count

# ----------- NOTIFICATIONS -----------
def insert_notification(alarm_event_id: int) -> int:
    """
    Fan-out 1 alarm_event thành notifications 'unread' cho TẤT CẢ users.
    Trả về số bản ghi MỚI được chèn (bỏ qua trùng nếu UNIQUE).
    """
    with init_engine().begin() as con:
        # 1) Lấy tất cả user_id
        user_rows = con.execute(select(users.c.id)).fetchall()
        user_ids = [r[0] for r in user_rows]
        if not user_ids:
            return 0

        # 2) Chuẩn bị rows
        rows = [{
            "alarm_event_id": alarm_event_id,
            "user_id": uid,
            "status": "unread",   # 'unread' == unseen
            "seen_at": None,
            "dismissed_at": None,
        } for uid in user_ids]

        # 3) Bulk insert (idempotent)
        inserted = 0
        try:
            con.execute(notifications.insert(), rows)  # executemany
            inserted = len(rows)
        except IntegrityError:
            # Rơi vào đây khi đã có UNIQUE(alarm_event_id, user_id) trùng
            for r in rows:
                try:
                    con.execute(notifications.insert().values(**r))
                    inserted += 1
                except IntegrityError:
                    pass
        return inserted


def get_notifications(
    user_id: int,
    *,
    status: str = "all",   # 'unread' | 'seen' | 'dismissed' | 'all'
    limit: int = 50,
    offset: int = 0
) -> List[dict]:
    """
    Lấy danh sách notifications cho 1 user, kèm thông tin alarm_events.
    Mặc định ẩn 'dismissed' khi status='all'.
    """
    with init_engine().begin() as con:
        # Base select
        stmt = (
            select(
                notifications.c.id.label("notification_id"),
                notifications.c.status,
                notifications.c.seen_at,
                notifications.c.dismissed_at,
                notifications.c.created_at,

                alarm_events.c.id.label("alarm_event_id"),
                alarm_events.c.ts,
                alarm_events.c.name,
                alarm_events.c.level,
                alarm_events.c.target,
                alarm_events.c.value,
                alarm_events.c.note,
                alarm_events.c.event_type,
                alarm_events.c.operator,
                alarm_events.c.threshold,
            )
            .select_from(
                notifications.join(alarm_events, notifications.c.alarm_event_id == alarm_events.c.id)
            )
            .where(notifications.c.user_id == user_id)
            .order_by(notifications.c.id.desc())
            .limit(limit)
            .offset(offset)
        )

        # Lọc theo status
        if status in ("unread", "seen", "dismissed"):
            stmt = stmt.where(notifications.c.status == status)
        elif status == "all":
            # Ẩn dismissed mặc định
            stmt = stmt.where(notifications.c.status != "dismissed")

        rows = con.execute(stmt).mappings().all()
        return [dict(r) for r in rows]


def mark_user_notifications_read(user_id: int) -> bool:
    """
    Đánh dấu tất cả notifications của user đã đọc (status = 'seen')
    """
    try:
        with init_engine().begin() as con:
            stmt = (
                update(notifications)
                .where(
                    and_(
                        notifications.c.user_id == user_id,
                        notifications.c.status == "unread"
                    )
                )
                .values(
                    status="seen",
                    seen_at=safe_datetime_now()
                )
            )
            result = con.execute(stmt)
            return result.rowcount > 0
    except Exception as e:
        print(f"Error marking notifications as read: {e}")
        return False


def mark_user_notifications_dismissed(user_id: int) -> bool:
    """
    Đánh dấu tất cả notifications của user là 'dismissed' để không trả về nữa.
    """
    try:
        with init_engine().begin() as con:
            stmt = (
                update(notifications)
                .where(
                    and_(
                        notifications.c.user_id == user_id,
                        notifications.c.status != "dismissed"
                    )
                )
                .values(
                    status="dismissed",
                    dismissed_at=safe_datetime_now()
                )
            )
            result = con.execute(stmt)
            return result.rowcount > 0
    except Exception as e:
        print(f"Error dismissing notifications: {e}")
        return False

# ========== QUAD TAG CARDS OPERATIONS ==========

def add_quad_tag_card(group_id: int, tag1_id: int, tag2_id: int,
                       tag3_id=None, tag4_id=None,
                       card_title: str = None, left_title: str = None, right_title: str = None,
                       sv_left_type='tag', sv_left_fixed=None,
                       sv_right_type='tag', sv_right_fixed=None) -> int:
    """Add a new quad tag card to a group. SV tags support fix value mode."""
    with init_engine().begin() as con:
        res = con.execute(
            insert(subdash_quad_cards).values(
                group_id=group_id,
                tag1_id=tag1_id,
                tag2_id=tag2_id,
                tag3_id=tag3_id,
                tag4_id=tag4_id,
                sv_left_type=sv_left_type, sv_left_fixed=sv_left_fixed,
                sv_right_type=sv_right_type, sv_right_fixed=sv_right_fixed,
                card_title=card_title,
                left_title=left_title,
                right_title=right_title
            )
        )
        return res.inserted_primary_key[0]

def get_quad_cards_for_group(group_id: int):
    """Get all quad cards for a specific group with tag details and resolved SV values."""
    with init_engine().connect() as con:
        quad_cards_query = select(subdash_quad_cards).where(subdash_quad_cards.c.group_id == group_id)
        quad_cards = con.execute(quad_cards_query).mappings().all()

        sv_map = {
            3: ('sv_left_type', 'sv_left_fixed'),
            4: ('sv_right_type', 'sv_right_fixed'),
        }

        result = []
        for card in quad_cards:
            card_dict = dict(card)

            # PV tags (tag1, tag2) - always from tags table
            for pos in [1, 2]:
                tag_id = card_dict[f'tag{pos}_id']
                tag = con.execute(select(tags).where(tags.c.id == tag_id)).mappings().first()
                card_dict[f'tag{pos}'] = dict(tag) if tag else None
                if tag:
                    last_value, _ = get_latest_tag_value(tag_id)
                    card_dict[f'tag{pos}']['last_value'] = last_value if last_value is not None else 0

            # SV tags (tag3, tag4) - support fix value mode
            for pos in [3, 4]:
                type_col, fixed_col = sv_map[pos]
                sv_type = card_dict.get(type_col) or 'tag'
                tag_id = card_dict.get(f'tag{pos}_id')

                sv_value = _resolve_sv_value(con, sv_type, tag_id, card_dict.get(fixed_col))
                card_dict[f'tag{pos}_sv_value'] = sv_value

                if sv_type == 'tag' and tag_id:
                    tag = con.execute(select(tags).where(tags.c.id == tag_id)).mappings().first()
                    card_dict[f'tag{pos}'] = dict(tag) if tag else None
                    if tag:
                        last_value, _ = get_latest_tag_value(tag_id)
                        card_dict[f'tag{pos}']['last_value'] = last_value if last_value is not None else 0
                else:
                    card_dict[f'tag{pos}'] = None

            result.append(card_dict)

        return result

def delete_quad_card(quad_card_id: int) -> bool:
    """Delete a quad tag card."""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(subdash_quad_cards).where(subdash_quad_cards.c.id == quad_card_id)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting quad card {quad_card_id}: {e}")
        return False

def get_quad_card_by_id(quad_card_id: int):
    """Get a specific quad card by ID."""
    with init_engine().connect() as con:
        result = con.execute(
            select(subdash_quad_cards).where(subdash_quad_cards.c.id == quad_card_id)
        ).mappings().first()
        return dict(result) if result else None

def update_quad_card(quad_card_id: int, tag1_id: int, tag2_id: int,
                     tag3_id=None, tag4_id=None,
                     card_title: str = None, left_title: str = None, right_title: str = None,
                     sv_left_type='tag', sv_left_fixed=None,
                     sv_right_type='tag', sv_right_fixed=None) -> bool:
    """Update a quad tag card. SV tags support fix value mode."""
    try:
        with init_engine().begin() as con:
            values_dict = {
                'tag1_id': tag1_id,
                'tag2_id': tag2_id,
                'tag3_id': tag3_id,
                'tag4_id': tag4_id,
                'sv_left_type': sv_left_type,
                'sv_left_fixed': sv_left_fixed,
                'sv_right_type': sv_right_type,
                'sv_right_fixed': sv_right_fixed,
            }

            if card_title is not None:
                values_dict['card_title'] = card_title
            if left_title is not None:
                values_dict['left_title'] = left_title
            if right_title is not None:
                values_dict['right_title'] = right_title
            
            result = con.execute(
                update(subdash_quad_cards)
                .where(subdash_quad_cards.c.id == quad_card_id)
                .values(**values_dict)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating quad card {quad_card_id}: {e}")
        return False

def update_quad_card_titles(quad_card_id: int, card_title: str = None, left_title: str = None, right_title: str = None) -> bool:
    """Update only the titles of a quad tag card."""
    try:
        with init_engine().begin() as con:
            values_dict = {}
            
            if card_title is not None:
                values_dict['card_title'] = card_title
            if left_title is not None:
                values_dict['left_title'] = left_title
            if right_title is not None:
                values_dict['right_title'] = right_title
            
            if not values_dict:
                return True  # Nothing to update
            
            result = con.execute(
                update(subdash_quad_cards)
                .where(subdash_quad_cards.c.id == quad_card_id)
                .values(**values_dict)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating quad card titles {quad_card_id}: {e}")
        return False


# ========== QTAG6 CARD CRUD OPERATIONS ==========

def add_qtag6_card(group_id: int, tag1_id: int, tag2_id: int,
                   tag3_id=None, tag4_id=None, tag5_id=None, tag6_id=None,
                   card_title: str = None, left_title: str = None, right_title: str = None,
                   left_sv_high_type='tag', left_sv_high_fixed=None,
                   right_sv_high_type='tag', right_sv_high_fixed=None,
                   left_sv_low_type='tag', left_sv_low_fixed=None,
                   right_sv_low_type='tag', right_sv_low_fixed=None) -> int:
    """Add a new qtag6 card (6 tags) to a group. SV tags support fix value mode."""
    with init_engine().begin() as con:
        res = con.execute(
            insert(subdash_qtag6_cards).values(
                group_id=group_id,
                tag1_id=tag1_id, tag2_id=tag2_id,
                tag3_id=tag3_id, tag4_id=tag4_id,
                tag5_id=tag5_id, tag6_id=tag6_id,
                left_sv_high_type=left_sv_high_type, left_sv_high_fixed=left_sv_high_fixed,
                right_sv_high_type=right_sv_high_type, right_sv_high_fixed=right_sv_high_fixed,
                left_sv_low_type=left_sv_low_type, left_sv_low_fixed=left_sv_low_fixed,
                right_sv_low_type=right_sv_low_type, right_sv_low_fixed=right_sv_low_fixed,
                card_title=card_title,
                left_title=left_title,
                right_title=right_title
            )
        )
        return res.inserted_primary_key[0]


def get_qtag6_cards_for_group(group_id: int):
    """Get all qtag6 cards for a specific group with tag details and resolved SV values."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(subdash_qtag6_cards).where(subdash_qtag6_cards.c.group_id == group_id)
        ).mappings().all()

        # Map tag position -> SV type/fixed column names
        sv_map = {
            3: ('left_sv_high_type', 'left_sv_high_fixed'),
            4: ('right_sv_high_type', 'right_sv_high_fixed'),
            5: ('left_sv_low_type', 'left_sv_low_fixed'),
            6: ('right_sv_low_type', 'right_sv_low_fixed'),
        }

        result = []
        for card in rows:
            card_dict = dict(card)
            # PV tags (tag1, tag2) - always from tags table
            for pos in [1, 2]:
                tag_id = card_dict[f'tag{pos}_id']
                tag = con.execute(select(tags).where(tags.c.id == tag_id)).mappings().first()
                card_dict[f'tag{pos}'] = dict(tag) if tag else None
                if tag:
                    last_value, _ = get_latest_tag_value(tag_id)
                    card_dict[f'tag{pos}']['last_value'] = last_value if last_value is not None else 0

            # SV tags (tag3-6) - support fix value mode
            for pos in [3, 4, 5, 6]:
                type_col, fixed_col = sv_map[pos]
                sv_type = card_dict.get(type_col) or 'tag'  # default 'tag' for backward compat
                tag_id = card_dict.get(f'tag{pos}_id')

                # Resolve SV value
                sv_value = _resolve_sv_value(con, sv_type, tag_id,
                                             card_dict.get(fixed_col))
                card_dict[f'tag{pos}_sv_value'] = sv_value

                # Load tag info if type is 'tag'
                if sv_type == 'tag' and tag_id:
                    tag = con.execute(select(tags).where(tags.c.id == tag_id)).mappings().first()
                    card_dict[f'tag{pos}'] = dict(tag) if tag else None
                    if tag:
                        last_value, _ = get_latest_tag_value(tag_id)
                        card_dict[f'tag{pos}']['last_value'] = last_value if last_value is not None else 0
                else:
                    card_dict[f'tag{pos}'] = None

            result.append(card_dict)
        return result


def get_qtag6_card_by_id(card_id: int):
    """Get a specific qtag6 card by ID."""
    with init_engine().connect() as con:
        row = con.execute(
            select(subdash_qtag6_cards).where(subdash_qtag6_cards.c.id == card_id)
        ).mappings().first()
        return dict(row) if row else None


def update_qtag6_card(card_id: int, **kwargs) -> bool:
    """Update a qtag6 card. Pass only fields to update."""
    try:
        allowed = {'tag1_id', 'tag2_id', 'tag3_id', 'tag4_id', 'tag5_id', 'tag6_id',
                    'card_title', 'left_title', 'right_title', 'card_color',
                    'left_sv_high_type', 'left_sv_high_fixed',
                    'right_sv_high_type', 'right_sv_high_fixed',
                    'left_sv_low_type', 'left_sv_low_fixed',
                    'right_sv_low_type', 'right_sv_low_fixed'}
        values_dict = {k: v for k, v in kwargs.items() if k in allowed}
        if not values_dict:
            return True
        with init_engine().begin() as con:
            result = con.execute(
                update(subdash_qtag6_cards)
                .where(subdash_qtag6_cards.c.id == card_id)
                .values(**values_dict)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating qtag6 card {card_id}: {e}")
        return False


def delete_qtag6_card(card_id: int) -> bool:
    """Delete a qtag6 card."""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(subdash_qtag6_cards).where(subdash_qtag6_cards.c.id == card_id)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting qtag6 card {card_id}: {e}")
        return False


# ========== QTAG4 CARD CRUD OPERATIONS (2 columns: PV + SV each) ==========

def add_qtag4_card(group_id: int, tag1_id: int, tag2_id: int,
                   tag3_id=None, tag4_id=None,
                   card_title: str = None, left_title: str = None, right_title: str = None,
                   left_sv_type='tag', left_sv_fixed=None,
                   right_sv_type='tag', right_sv_fixed=None) -> int:
    """Add a new qtag4 card (4 tags: 2 PV + 2 SV) to a group."""
    with init_engine().begin() as con:
        res = con.execute(
            insert(subdash_qtag4_cards).values(
                group_id=group_id,
                tag1_id=tag1_id, tag2_id=tag2_id,
                tag3_id=tag3_id, tag4_id=tag4_id,
                left_sv_type=left_sv_type, left_sv_fixed=left_sv_fixed,
                right_sv_type=right_sv_type, right_sv_fixed=right_sv_fixed,
                card_title=card_title,
                left_title=left_title,
                right_title=right_title
            )
        )
        return res.inserted_primary_key[0]


def get_qtag4_cards_for_group(group_id: int):
    """Get all qtag4 cards for a specific group with tag details and resolved SV values."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(subdash_qtag4_cards).where(subdash_qtag4_cards.c.group_id == group_id)
        ).mappings().all()

        sv_map = {
            3: ('left_sv_type', 'left_sv_fixed'),
            4: ('right_sv_type', 'right_sv_fixed'),
        }

        result = []
        for card in rows:
            card_dict = dict(card)
            # PV tags (tag1, tag2) - always from tags table
            for pos in [1, 2]:
                tag_id = card_dict[f'tag{pos}_id']
                tag = con.execute(select(tags).where(tags.c.id == tag_id)).mappings().first()
                card_dict[f'tag{pos}'] = dict(tag) if tag else None
                if tag:
                    last_value, _ = get_latest_tag_value(tag_id)
                    card_dict[f'tag{pos}']['last_value'] = last_value if last_value is not None else 0

            # SV tags (tag3, tag4) - support fix value mode
            for pos in [3, 4]:
                type_col, fixed_col = sv_map[pos]
                sv_type = card_dict.get(type_col) or 'tag'
                tag_id = card_dict.get(f'tag{pos}_id')
                sv_value = _resolve_sv_value(con, sv_type, tag_id, card_dict.get(fixed_col))
                card_dict[f'tag{pos}_sv_value'] = sv_value

                if sv_type == 'tag' and tag_id:
                    tag = con.execute(select(tags).where(tags.c.id == tag_id)).mappings().first()
                    card_dict[f'tag{pos}'] = dict(tag) if tag else None
                    if tag:
                        last_value, _ = get_latest_tag_value(tag_id)
                        card_dict[f'tag{pos}']['last_value'] = last_value if last_value is not None else 0
                else:
                    card_dict[f'tag{pos}'] = None

            result.append(card_dict)
        return result


def get_qtag4_card_by_id(card_id: int):
    """Get a specific qtag4 card by ID."""
    with init_engine().connect() as con:
        row = con.execute(
            select(subdash_qtag4_cards).where(subdash_qtag4_cards.c.id == card_id)
        ).mappings().first()
        return dict(row) if row else None


def update_qtag4_card(card_id: int, **kwargs) -> bool:
    """Update a qtag4 card. Pass only fields to update."""
    try:
        allowed = {'tag1_id', 'tag2_id', 'tag3_id', 'tag4_id',
                    'card_title', 'left_title', 'right_title', 'card_color',
                    'left_sv_type', 'left_sv_fixed',
                    'right_sv_type', 'right_sv_fixed'}
        values_dict = {k: v for k, v in kwargs.items() if k in allowed}
        if not values_dict:
            return True
        with init_engine().begin() as con:
            result = con.execute(
                update(subdash_qtag4_cards)
                .where(subdash_qtag4_cards.c.id == card_id)
                .values(**values_dict)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating qtag4 card {card_id}: {e}")
        return False


def delete_qtag4_card(card_id: int) -> bool:
    """Delete a qtag4 card."""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(subdash_qtag4_cards).where(subdash_qtag4_cards.c.id == card_id)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting qtag4 card {card_id}: {e}")
        return False


# ========== QTAG3 CARD CRUD OPERATIONS (1 column: PV + SV HIGH + SV LOW) ==========

def add_qtag3_card(group_id: int, tag1_id: int,
                   tag2_id=None, tag3_id=None,
                   card_title: str = None, column_title: str = None,
                   sv_high_type='tag', sv_high_fixed=None,
                   sv_low_type='tag', sv_low_fixed=None) -> int:
    """Add a new qtag3 card (1 column: PV + SV HIGH + SV LOW)."""
    with init_engine().begin() as con:
        res = con.execute(
            insert(subdash_qtag3_cards).values(
                group_id=group_id,
                tag1_id=tag1_id, tag2_id=tag2_id, tag3_id=tag3_id,
                sv_high_type=sv_high_type, sv_high_fixed=sv_high_fixed,
                sv_low_type=sv_low_type, sv_low_fixed=sv_low_fixed,
                card_title=card_title, column_title=column_title
            )
        )
        return res.inserted_primary_key[0]


def get_qtag3_cards_for_group(group_id: int):
    """Get all qtag3 cards for a group with tag details and resolved SV values."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(subdash_qtag3_cards).where(subdash_qtag3_cards.c.group_id == group_id)
        ).mappings().all()

        result = []
        for card in rows:
            card_dict = dict(card)
            # PV tag (tag1) - always from tags table
            tag_id = card_dict['tag1_id']
            tag = con.execute(select(tags).where(tags.c.id == tag_id)).mappings().first()
            card_dict['tag1'] = dict(tag) if tag else None
            if tag:
                last_value, _ = get_latest_tag_value(tag_id)
                card_dict['tag1']['last_value'] = last_value if last_value is not None else 0

            # SV HIGH (tag2) - support fix value mode
            sv_high_type = card_dict.get('sv_high_type') or 'tag'
            tag2_id = card_dict.get('tag2_id')
            card_dict['tag2_sv_value'] = _resolve_sv_value(con, sv_high_type, tag2_id, card_dict.get('sv_high_fixed'))
            if sv_high_type == 'tag' and tag2_id:
                tag = con.execute(select(tags).where(tags.c.id == tag2_id)).mappings().first()
                card_dict['tag2'] = dict(tag) if tag else None
                if tag:
                    last_value, _ = get_latest_tag_value(tag2_id)
                    card_dict['tag2']['last_value'] = last_value if last_value is not None else 0
            else:
                card_dict['tag2'] = None

            # SV LOW (tag3) - support fix value mode
            sv_low_type = card_dict.get('sv_low_type') or 'tag'
            tag3_id = card_dict.get('tag3_id')
            card_dict['tag3_sv_value'] = _resolve_sv_value(con, sv_low_type, tag3_id, card_dict.get('sv_low_fixed'))
            if sv_low_type == 'tag' and tag3_id:
                tag = con.execute(select(tags).where(tags.c.id == tag3_id)).mappings().first()
                card_dict['tag3'] = dict(tag) if tag else None
                if tag:
                    last_value, _ = get_latest_tag_value(tag3_id)
                    card_dict['tag3']['last_value'] = last_value if last_value is not None else 0
            else:
                card_dict['tag3'] = None

            result.append(card_dict)
        return result


def get_qtag3_card_by_id(card_id: int):
    """Get a specific qtag3 card by ID."""
    with init_engine().connect() as con:
        row = con.execute(
            select(subdash_qtag3_cards).where(subdash_qtag3_cards.c.id == card_id)
        ).mappings().first()
        return dict(row) if row else None


def update_qtag3_card(card_id: int, **kwargs) -> bool:
    """Update a qtag3 card. Pass only fields to update."""
    try:
        allowed = {'tag1_id', 'tag2_id', 'tag3_id',
                    'card_title', 'column_title', 'card_color',
                    'sv_high_type', 'sv_high_fixed',
                    'sv_low_type', 'sv_low_fixed'}
        values_dict = {k: v for k, v in kwargs.items() if k in allowed}
        if not values_dict:
            return True
        with init_engine().begin() as con:
            result = con.execute(
                update(subdash_qtag3_cards)
                .where(subdash_qtag3_cards.c.id == card_id)
                .values(**values_dict)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating qtag3 card {card_id}: {e}")
        return False


def delete_qtag3_card(card_id: int) -> bool:
    """Delete a qtag3 card."""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(subdash_qtag3_cards).where(subdash_qtag3_cards.c.id == card_id)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting qtag3 card {card_id}: {e}")
        return False


# ========== QTAG2 CARD CRUD OPERATIONS ==========

def add_qtag2_card(group_id: int, tag1_id: int,
                   tag2_id=None, card_title: str = None,
                   column_title: str = None,
                   sv_type='tag', sv_fixed=None) -> int:
    """Add a new qtag2 card (1 column: PV + SV)."""
    with init_engine().begin() as con:
        res = con.execute(
            insert(subdash_qtag2_cards).values(
                group_id=group_id,
                tag1_id=tag1_id, tag2_id=tag2_id,
                sv_type=sv_type, sv_fixed=sv_fixed,
                card_title=card_title, column_title=column_title
            )
        )
        return res.inserted_primary_key[0]


def get_qtag2_cards_for_group(group_id: int):
    """Get all qtag2 cards for a group with tag details and resolved SV value."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(subdash_qtag2_cards).where(subdash_qtag2_cards.c.group_id == group_id)
        ).mappings().all()

        result = []
        for card in rows:
            card_dict = dict(card)
            # PV tag (tag1) - always from tags table
            tag_id = card_dict['tag1_id']
            tag = con.execute(select(tags).where(tags.c.id == tag_id)).mappings().first()
            card_dict['tag1'] = dict(tag) if tag else None
            if tag:
                last_value, _ = get_latest_tag_value(tag_id)
                card_dict['tag1']['last_value'] = last_value if last_value is not None else 0

            # SV (tag2) - support fix value mode
            sv_type = card_dict.get('sv_type') or 'tag'
            tag2_id = card_dict.get('tag2_id')
            card_dict['tag2_sv_value'] = _resolve_sv_value(con, sv_type, tag2_id, card_dict.get('sv_fixed'))
            if sv_type == 'tag' and tag2_id:
                tag = con.execute(select(tags).where(tags.c.id == tag2_id)).mappings().first()
                card_dict['tag2'] = dict(tag) if tag else None
                if tag:
                    last_value, _ = get_latest_tag_value(tag2_id)
                    card_dict['tag2']['last_value'] = last_value if last_value is not None else 0
            else:
                card_dict['tag2'] = None

            result.append(card_dict)
        return result


def get_qtag2_card_by_id(card_id: int):
    """Get a specific qtag2 card by ID."""
    with init_engine().connect() as con:
        row = con.execute(
            select(subdash_qtag2_cards).where(subdash_qtag2_cards.c.id == card_id)
        ).mappings().first()
        return dict(row) if row else None


def update_qtag2_card(card_id: int, **kwargs) -> bool:
    """Update a qtag2 card. Pass only fields to update."""
    try:
        allowed = {'tag1_id', 'tag2_id',
                    'card_title', 'column_title', 'card_color',
                    'sv_type', 'sv_fixed'}
        values_dict = {k: v for k, v in kwargs.items() if k in allowed}
        if not values_dict:
            return True
        with init_engine().begin() as con:
            result = con.execute(
                update(subdash_qtag2_cards)
                .where(subdash_qtag2_cards.c.id == card_id)
                .values(**values_dict)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating qtag2 card {card_id}: {e}")
        return False


def delete_qtag2_card(card_id: int) -> bool:
    """Delete a qtag2 card."""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(subdash_qtag2_cards).where(subdash_qtag2_cards.c.id == card_id)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting qtag2 card {card_id}: {e}")
        return False


# ========== QTAG SINGLE3 CARD CRUD OPERATIONS ==========

def _resolve_sv_value(con, sv_type: str, tag_id, fixed_value):
    """Helper: resolve SV value — if type=='tag' return real-time tag value, else return fixed."""
    if sv_type == 'tag' and tag_id:
        last_value, _ = get_latest_tag_value(int(tag_id))
        return last_value if last_value is not None else None
    elif sv_type == 'fixed' and fixed_value is not None:
        return float(fixed_value)
    return None


def add_qtag_single3_card(group_id: int, pv_tag_id: int, card_title: str = None,
                          sv_high_type: str = 'fixed', sv_high_tag_id: int = None,
                          sv_high_fixed: float = None,
                          sv_low_type: str = 'fixed', sv_low_tag_id: int = None,
                          sv_low_fixed: float = None) -> int:
    """Add a new qtag single3 card (PV + SV HIGH/LOW) to a group."""
    with init_engine().begin() as con:
        res = con.execute(
            insert(subdash_qtag_single3_cards).values(
                group_id=group_id,
                card_title=card_title,
                pv_tag_id=pv_tag_id,
                sv_high_type=sv_high_type,
                sv_high_tag_id=sv_high_tag_id,
                sv_high_fixed=sv_high_fixed,
                sv_low_type=sv_low_type,
                sv_low_tag_id=sv_low_tag_id,
                sv_low_fixed=sv_low_fixed,
            )
        )
        return res.inserted_primary_key[0]


def get_qtag_single3_cards_for_group(group_id: int):
    """Get all qtag single3 cards for a group with resolved values."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(subdash_qtag_single3_cards)
            .where(subdash_qtag_single3_cards.c.group_id == group_id)
        ).mappings().all()

        result = []
        for card in rows:
            cd = dict(card)
            # Resolve PV tag
            pv_tag = con.execute(select(tags).where(tags.c.id == cd['pv_tag_id'])).mappings().first()
            cd['pv_tag'] = dict(pv_tag) if pv_tag else None
            if pv_tag:
                last_value, _ = get_latest_tag_value(cd['pv_tag_id'])
                cd['pv_tag']['last_value'] = last_value if last_value is not None else 0

            # Resolve SV HIGH
            cd['sv_high_value'] = _resolve_sv_value(con, cd.get('sv_high_type', 'fixed'),
                                                     cd.get('sv_high_tag_id'), cd.get('sv_high_fixed'))
            if cd.get('sv_high_type') == 'tag' and cd.get('sv_high_tag_id'):
                sv_h_tag = con.execute(select(tags).where(tags.c.id == cd['sv_high_tag_id'])).mappings().first()
                cd['sv_high_tag'] = dict(sv_h_tag) if sv_h_tag else None
            else:
                cd['sv_high_tag'] = None

            # Resolve SV LOW
            cd['sv_low_value'] = _resolve_sv_value(con, cd.get('sv_low_type', 'fixed'),
                                                    cd.get('sv_low_tag_id'), cd.get('sv_low_fixed'))
            if cd.get('sv_low_type') == 'tag' and cd.get('sv_low_tag_id'):
                sv_l_tag = con.execute(select(tags).where(tags.c.id == cd['sv_low_tag_id'])).mappings().first()
                cd['sv_low_tag'] = dict(sv_l_tag) if sv_l_tag else None
            else:
                cd['sv_low_tag'] = None

            result.append(cd)
        return result


def get_qtag_single3_card_by_id(card_id: int):
    """Get a specific qtag single3 card by ID."""
    with init_engine().connect() as con:
        row = con.execute(
            select(subdash_qtag_single3_cards).where(subdash_qtag_single3_cards.c.id == card_id)
        ).mappings().first()
        return dict(row) if row else None


def update_qtag_single3_card(card_id: int, **kwargs) -> bool:
    """Update a qtag single3 card."""
    try:
        allowed = {'card_title', 'pv_tag_id', 'card_color',
                    'sv_high_type', 'sv_high_tag_id', 'sv_high_fixed',
                    'sv_low_type', 'sv_low_tag_id', 'sv_low_fixed'}
        values_dict = {k: v for k, v in kwargs.items() if k in allowed}
        if not values_dict:
            return True
        with init_engine().begin() as con:
            result = con.execute(
                update(subdash_qtag_single3_cards)
                .where(subdash_qtag_single3_cards.c.id == card_id)
                .values(**values_dict)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating qtag single3 card {card_id}: {e}")
        return False


def delete_qtag_single3_card(card_id: int) -> bool:
    """Delete a qtag single3 card."""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(subdash_qtag_single3_cards).where(subdash_qtag_single3_cards.c.id == card_id)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting qtag single3 card {card_id}: {e}")
        return False


# ========== QTAG PV ONLY CARD CRUD OPERATIONS ==========

def add_qtag_pv_card(group_id: int, pv_tag_id: int, card_title: str = None) -> int:
    """Add a new qtag PV only card to a group."""
    with init_engine().begin() as con:
        res = con.execute(
            insert(subdash_qtag_pv_cards).values(
                group_id=group_id,
                card_title=card_title,
                pv_tag_id=pv_tag_id,
            )
        )
        return res.inserted_primary_key[0]


def get_qtag_pv_cards_for_group(group_id: int):
    """Get all qtag PV only cards for a group with tag details."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(subdash_qtag_pv_cards).where(subdash_qtag_pv_cards.c.group_id == group_id)
        ).mappings().all()

        result = []
        for card in rows:
            cd = dict(card)
            pv_tag = con.execute(select(tags).where(tags.c.id == cd['pv_tag_id'])).mappings().first()
            cd['pv_tag'] = dict(pv_tag) if pv_tag else None
            if pv_tag:
                last_value, _ = get_latest_tag_value(cd['pv_tag_id'])
                cd['pv_tag']['last_value'] = last_value if last_value is not None else 0
            result.append(cd)
        return result


def get_qtag_pv_card_by_id(card_id: int):
    """Get a specific qtag PV only card by ID."""
    with init_engine().connect() as con:
        row = con.execute(
            select(subdash_qtag_pv_cards).where(subdash_qtag_pv_cards.c.id == card_id)
        ).mappings().first()
        return dict(row) if row else None


def update_qtag_pv_card(card_id: int, **kwargs) -> bool:
    """Update a qtag PV only card."""
    try:
        allowed = {'card_title', 'pv_tag_id', 'card_color'}
        values_dict = {k: v for k, v in kwargs.items() if k in allowed}
        if not values_dict:
            return True
        with init_engine().begin() as con:
            result = con.execute(
                update(subdash_qtag_pv_cards)
                .where(subdash_qtag_pv_cards.c.id == card_id)
                .values(**values_dict)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating qtag PV card {card_id}: {e}")
        return False


def delete_qtag_pv_card(card_id: int) -> bool:
    """Delete a qtag PV only card."""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(subdash_qtag_pv_cards).where(subdash_qtag_pv_cards.c.id == card_id)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting qtag PV card {card_id}: {e}")
        return False


# ========== QTAG PV DUAL CARD CRUD OPERATIONS ==========

def add_qtag_pv_dual_card(group_id: int, left_tag_id: int, right_tag_id: int,
                          card_title: str = None, left_title: str = None, right_title: str = None) -> int:
    """Add a new qtag PV dual card to a group."""
    with init_engine().begin() as con:
        res = con.execute(
            insert(subdash_qtag_pv_dual_cards).values(
                group_id=group_id,
                card_title=card_title,
                left_title=left_title,
                right_title=right_title,
                left_tag_id=left_tag_id,
                right_tag_id=right_tag_id,
            )
        )
        return res.inserted_primary_key[0]


def get_qtag_pv_dual_cards_for_group(group_id: int):
    """Get all qtag PV dual cards for a group with tag details."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(subdash_qtag_pv_dual_cards).where(subdash_qtag_pv_dual_cards.c.group_id == group_id)
        ).mappings().all()

        result = []
        for card in rows:
            cd = dict(card)
            # Left tag
            left_tag = con.execute(select(tags).where(tags.c.id == cd['left_tag_id'])).mappings().first()
            cd['left_tag'] = dict(left_tag) if left_tag else None
            if left_tag:
                last_value, _ = get_latest_tag_value(cd['left_tag_id'])
                cd['left_tag']['last_value'] = last_value if last_value is not None else 0
            # Right tag
            right_tag = con.execute(select(tags).where(tags.c.id == cd['right_tag_id'])).mappings().first()
            cd['right_tag'] = dict(right_tag) if right_tag else None
            if right_tag:
                last_value, _ = get_latest_tag_value(cd['right_tag_id'])
                cd['right_tag']['last_value'] = last_value if last_value is not None else 0
            result.append(cd)
        return result


def get_qtag_pv_dual_card_by_id(card_id: int):
    """Get a specific qtag PV dual card by ID."""
    with init_engine().connect() as con:
        row = con.execute(
            select(subdash_qtag_pv_dual_cards).where(subdash_qtag_pv_dual_cards.c.id == card_id)
        ).mappings().first()
        return dict(row) if row else None


def update_qtag_pv_dual_card(card_id: int, **kwargs) -> bool:
    """Update a qtag PV dual card."""
    try:
        allowed = {'card_title', 'left_title', 'right_title', 'left_tag_id', 'right_tag_id', 'card_color'}
        values_dict = {k: v for k, v in kwargs.items() if k in allowed}
        if not values_dict:
            return True
        with init_engine().begin() as con:
            result = con.execute(
                update(subdash_qtag_pv_dual_cards)
                .where(subdash_qtag_pv_dual_cards.c.id == card_id)
                .values(**values_dict)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating qtag PV dual card {card_id}: {e}")
        return False


def delete_qtag_pv_dual_card(card_id: int) -> bool:
    """Delete a qtag PV dual card."""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(subdash_qtag_pv_dual_cards).where(subdash_qtag_pv_dual_cards.c.id == card_id)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting qtag PV dual card {card_id}: {e}")
        return False


def update_card_color(card_type: str, card_id: int, color: str) -> bool:
    """Update card_color for any card type. color can be hex like '#ff0000' or None to reset."""
    import re
    table_map = {
        'quad': subdash_quad_cards,
        'quad6': subdash_qtag6_cards,
        'single3': subdash_qtag_single3_cards,
        'pvonly': subdash_qtag_pv_cards,
        'pvdual': subdash_qtag_pv_dual_cards,
    }
    tbl = table_map.get(card_type)
    if tbl is None:
        return False
    # Validate color format (hex color or None/empty to clear)
    if color and not re.match(r'^#[0-9a-fA-F]{6}$', color):
        return False
    try:
        with init_engine().begin() as con:
            result = con.execute(
                update(tbl).where(tbl.c.id == card_id).values(card_color=color or None)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error updating card color for {card_type}/{card_id}: {e}")
        return False


# ========== QUAD TAG CONDITIONS OPERATIONS ==========

def save_quad_condition(quad_card_id: int, conditions_data: dict) -> int:
    """
    Lưu hoặc cập nhật điều kiện alarm cho quad tag card.
    Trả về condition_id.
    """
    with init_engine().begin() as con:
        # Kiểm tra xem đã có condition cho quad card này chưa
        existing = con.execute(
            select(quad_tag_conditions.c.id)
            .where(quad_tag_conditions.c.quad_card_id == quad_card_id)
        ).first()
        
        if existing:
            # Update existing
            con.execute(
                update(quad_tag_conditions)
                .where(quad_tag_conditions.c.quad_card_id == quad_card_id)
                .values(**conditions_data)
            )
            return existing[0]
        else:
            # Insert new
            conditions_data['quad_card_id'] = quad_card_id
            result = con.execute(
                insert(quad_tag_conditions).values(**conditions_data)
            )
            return result.inserted_primary_key[0]


def get_quad_condition(quad_card_id: int):
    """Lấy điều kiện alarm của quad tag card."""
    with init_engine().connect() as con:
        result = con.execute(
            select(quad_tag_conditions)
            .where(quad_tag_conditions.c.quad_card_id == quad_card_id)
        ).mappings().first()
        return dict(result) if result else None


def delete_quad_condition(quad_card_id: int) -> bool:
    """Xóa điều kiện alarm của quad tag card."""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(quad_tag_conditions)
                .where(quad_tag_conditions.c.quad_card_id == quad_card_id)
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting quad condition for card {quad_card_id}: {e}")
        return False


# ===== User Report Comparison Config Functions =====

def save_user_comparison_config(user_id: int, logger_id: str, config_json: str) -> bool:
    """
    Lưu hoặc cập nhật cấu hình comparison của user cho logger cụ thể.
    
    Args:
        user_id: ID của user
        logger_id: ID của datalogger (hoặc 'all')
        config_json: JSON string chứa comparison pairs
    
    Returns:
        True nếu thành công, False nếu thất bại
    """
    try:
        with init_engine().begin() as con:
            # Check if config exists
            existing = con.execute(
                select(user_report_comparison_configs.c.id)
                .where(
                    (user_report_comparison_configs.c.user_id == user_id) &
                    (user_report_comparison_configs.c.logger_id == str(logger_id))
                )
            ).first()
            
            if existing:
                # Update existing config
                con.execute(
                    update(user_report_comparison_configs)
                    .where(
                        (user_report_comparison_configs.c.user_id == user_id) &
                        (user_report_comparison_configs.c.logger_id == str(logger_id))
                    )
                    .values(config_json=config_json, updated_at=func.now())
                )
            else:
                # Insert new config
                con.execute(
                    insert(user_report_comparison_configs)
                    .values(user_id=user_id, logger_id=str(logger_id), config_json=config_json)
                )
            return True
    except Exception as e:
        print(f"Error saving comparison config for user {user_id}, logger {logger_id}: {e}")
        return False


def get_user_comparison_config(user_id: int, logger_id: str) -> Optional[str]:
    """
    Lấy cấu hình comparison của user cho logger cụ thể.
    
    Args:
        user_id: ID của user
        logger_id: ID của datalogger (hoặc 'all')
    
    Returns:
        JSON string chứa comparison pairs hoặc None nếu không tìm thấy
    """
    try:
        with init_engine().connect() as con:
            result = con.execute(
                select(user_report_comparison_configs.c.config_json)
                .where(
                    (user_report_comparison_configs.c.user_id == user_id) &
                    (user_report_comparison_configs.c.logger_id == str(logger_id))
                )
            ).first()
            return result[0] if result else None
    except Exception as e:
        print(f"Error getting comparison config for user {user_id}, logger {logger_id}: {e}")
        return None


def delete_user_comparison_config(user_id: int, logger_id: str) -> bool:
    """
    Xóa cấu hình comparison của user cho logger cụ thể.
    
    Args:
        user_id: ID của user
        logger_id: ID của datalogger (hoặc 'all')
    
    Returns:
        True nếu thành công, False nếu thất bại
    """
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(user_report_comparison_configs)
                .where(
                    (user_report_comparison_configs.c.user_id == user_id) &
                    (user_report_comparison_configs.c.logger_id == str(logger_id))
                )
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting comparison config for user {user_id}, logger {logger_id}: {e}")
        return False


def get_all_active_quad_conditions():
    """
    Lấy tất cả điều kiện alarm đang active của tất cả quad cards.
    Dùng cho alarm worker để monitor.
    """
    with init_engine().connect() as con:
        result = con.execute(
            select(quad_tag_conditions)
            .where(quad_tag_conditions.c.enabled == True)
        ).mappings().all()
        return [dict(r) for r in result]


# ========== Card Alarm Conditions CRUD ==========

def get_card_alarm_condition(card_type: str, card_id: int):
    """Get alarm condition for a specific card."""
    with init_engine().connect() as con:
        result = con.execute(
            select(card_alarm_conditions)
            .where(
                (card_alarm_conditions.c.card_type == card_type) &
                (card_alarm_conditions.c.card_id == card_id)
            )
        ).mappings().first()
        return dict(result) if result else None


def save_card_alarm_condition(card_type: str, card_id: int, conditions_data: dict) -> int:
    """Save or update alarm condition for a card. Returns condition_id."""
    with init_engine().begin() as con:
        existing = con.execute(
            select(card_alarm_conditions.c.id)
            .where(
                (card_alarm_conditions.c.card_type == card_type) &
                (card_alarm_conditions.c.card_id == card_id)
            )
        ).first()

        if existing:
            con.execute(
                update(card_alarm_conditions)
                .where(
                    (card_alarm_conditions.c.card_type == card_type) &
                    (card_alarm_conditions.c.card_id == card_id)
                )
                .values(**conditions_data)
            )
            return existing[0]
        else:
            conditions_data['card_type'] = card_type
            conditions_data['card_id'] = card_id
            result = con.execute(
                insert(card_alarm_conditions).values(**conditions_data)
            )
            return result.inserted_primary_key[0]


def delete_card_alarm_condition(card_type: str, card_id: int) -> bool:
    """Delete alarm condition for a card."""
    try:
        with init_engine().begin() as con:
            result = con.execute(
                delete(card_alarm_conditions)
                .where(
                    (card_alarm_conditions.c.card_type == card_type) &
                    (card_alarm_conditions.c.card_id == card_id)
                )
            )
            return result.rowcount > 0
    except Exception as e:
        print(f"Error deleting card alarm condition for {card_type}/{card_id}: {e}")
        return False


def get_all_active_card_conditions():
    """Get all enabled card alarm conditions for alarm worker."""
    with init_engine().connect() as con:
        result = con.execute(
            select(card_alarm_conditions)
            .where(card_alarm_conditions.c.enabled == True)
        ).mappings().all()
        return [dict(r) for r in result]


def insert_card_alarm_state(card_type: str, card_id: int, column: str,
                            alarm_type: str, pv_value: float,
                            threshold: float, operator: str):
    """Insert or update card alarm state (UPSERT)."""
    try:
        with init_engine().begin() as con:
            con.execute(
                delete(card_alarm_states)
                .where(
                    (card_alarm_states.c.card_type == card_type) &
                    (card_alarm_states.c.card_id == card_id) &
                    (card_alarm_states.c.column == column)
                )
            )
            res = con.execute(
                card_alarm_states.insert().values(
                    card_type=card_type,
                    card_id=card_id,
                    column=column,
                    alarm_type=alarm_type,
                    triggered_at=datetime.now(),
                    pv_value=pv_value,
                    threshold=threshold,
                    operator=operator
                )
            )
            return res.inserted_primary_key[0] if res.inserted_primary_key else None
    except Exception as e:
        print(f"Error inserting card alarm state: {e}")
        return None


def delete_card_alarm_state(card_type: str, card_id: int, column: str):
    """Delete card alarm state when alarm clears."""
    try:
        with init_engine().begin() as con:
            res = con.execute(
                delete(card_alarm_states)
                .where(
                    (card_alarm_states.c.card_type == card_type) &
                    (card_alarm_states.c.card_id == card_id) &
                    (card_alarm_states.c.column == column)
                )
            )
            return res.rowcount
    except Exception as e:
        print(f"Error deleting card alarm state: {e}")
        return 0


def get_active_card_alarms():
    """Get all active card alarm states."""
    try:
        with init_engine().connect() as con:
            rows = con.execute(
                select(card_alarm_states).order_by(card_alarm_states.c.triggered_at.desc())
            ).mappings().all()
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error getting active card alarms: {e}")
        return []


def get_active_card_alarms_by_subdash(subdash_id: int):
    """Get active card alarm states for a specific subdashboard."""
    try:
        with init_engine().connect() as con:
            # Collect all card IDs by type for this subdashboard
            card_keys = []  # list of (card_type, card_id)

            # Qtag6
            rows = con.execute(
                select(subdash_qtag6_cards.c.id).select_from(
                    subdash_qtag6_cards.join(subdash_tag_groups, subdash_qtag6_cards.c.group_id == subdash_tag_groups.c.id)
                ).where(subdash_tag_groups.c.dashboard_id == subdash_id)
            ).fetchall()
            for r in rows:
                card_keys.append(('qtag6', r[0]))

            # Single3
            rows = con.execute(
                select(subdash_qtag_single3_cards.c.id).select_from(
                    subdash_qtag_single3_cards.join(subdash_tag_groups, subdash_qtag_single3_cards.c.group_id == subdash_tag_groups.c.id)
                ).where(subdash_tag_groups.c.dashboard_id == subdash_id)
            ).fetchall()
            for r in rows:
                card_keys.append(('single3', r[0]))

            # PV Only
            rows = con.execute(
                select(subdash_qtag_pv_cards.c.id).select_from(
                    subdash_qtag_pv_cards.join(subdash_tag_groups, subdash_qtag_pv_cards.c.group_id == subdash_tag_groups.c.id)
                ).where(subdash_tag_groups.c.dashboard_id == subdash_id)
            ).fetchall()
            for r in rows:
                card_keys.append(('pv_only', r[0]))

            # PV Dual
            rows = con.execute(
                select(subdash_qtag_pv_dual_cards.c.id).select_from(
                    subdash_qtag_pv_dual_cards.join(subdash_tag_groups, subdash_qtag_pv_dual_cards.c.group_id == subdash_tag_groups.c.id)
                ).where(subdash_tag_groups.c.dashboard_id == subdash_id)
            ).fetchall()
            for r in rows:
                card_keys.append(('pv_dual', r[0]))

            if not card_keys:
                return []

            # Build OR conditions for each (card_type, card_id) pair
            conditions = []
            for ct, cid in card_keys:
                conditions.append(
                    and_(
                        card_alarm_states.c.card_type == ct,
                        card_alarm_states.c.card_id == cid
                    )
                )

            from sqlalchemy import or_
            rows = con.execute(
                select(card_alarm_states).where(or_(*conditions))
                .order_by(card_alarm_states.c.triggered_at.desc())
            ).mappings().all()
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error getting active card alarms by subdash: {e}")
        import traceback
        traceback.print_exc()
        return []


def get_card_info_for_alarm(card_type: str, card_id: int):
    """Get card info with tag IDs for alarm evaluation.
    Returns dict with pv tag IDs depending on card type."""
    try:
        table_map = {
            'qtag6': subdash_qtag6_cards,
            'single3': subdash_qtag_single3_cards,
            'pv_only': subdash_qtag_pv_cards,
            'pv_dual': subdash_qtag_pv_dual_cards,
        }
        table = table_map.get(card_type)
        if not table:
            return None
        with init_engine().connect() as con:
            result = con.execute(
                select(table).where(table.c.id == card_id)
            ).mappings().first()
            return dict(result) if result else None
    except Exception as e:
        print(f"Error getting card info for alarm {card_type}/{card_id}: {e}")
        return None
    
