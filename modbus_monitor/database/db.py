
from ast import Dict, stmt
from typing import Optional
from typing import List as list
from typing import Tuple as tuple
from datetime import datetime, timedelta, timezone
import os
import threading
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float, Boolean,
    DateTime, Enum, ForeignKey, select, insert, update, delete, func,cast,and_, asc, text
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
    with open("config/SMTP_config.json") as config_file:
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
)

subdash_group_tags = Table(
    "subdash_group_tags", _md,
    Column("group_id", Integer, ForeignKey("subdash_tag_groups.id", ondelete="CASCADE"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), primary_key=True),
)



def create_schema():
    """Tạo bảng nếu chưa có (idempotent)."""
    engine = init_engine()
    _md.create_all(engine)
    # create_performance_indexes()


# ---------- CRUD NHANH (dùng trực tiếp trong route/service) ----------
def list_devices():
    with init_engine().connect() as con:
        rows = con.execute(select(devices)).mappings().all()
        # Check device online status based on updated_at and timeout_ms
        now = safe_datetime_now()
        all_devices = []
        for r in rows:
            updated_at = r.get("updated_at")
            timeout_ms = r.get("timeout_ms", 2000)
            device = dict(r)
            if updated_at:
                # Use safe datetime comparison
                time_diff = safe_datetime_compare(now, updated_at)
                if time_diff:
                    elapsed = time_diff.total_seconds() * 1000
                else:
                    elapsed = 0
                    
                if elapsed > timeout_ms:
                    device["is_online"] = False
                    update_device_row(device["id"], {"is_online": False, "updated_at": safe_datetime_now()})
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

def insert_tag_values_bulk(rows: list[tuple[int, "datetime", float]]):
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
    data = {k: v for k, v in data.items() if k in devices.c and v is not None}
    with init_engine().begin() as con:
        res = con.execute(update(devices).where(devices.c.id == device_id).values(**data))
        return res.rowcount

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
        con.execute(
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

# ----------- ALARM EVENTS (history) -----------
def list_alarm_events():
    """Return all alarm events, newest first."""
    with init_engine().connect() as con:
        rows = con.execute(select(alarm_events).order_by(alarm_events.c.ts.desc())).mappings().all()
        return [dict(r) for r in rows]

def list_alarm_events_by_date_range(start_date, end_date):
    """Return alarm events within date range, newest first."""
    with init_engine().connect() as con:
        rows = con.execute(
            select(alarm_events)
            .where(alarm_events.c.ts >= start_date)
            .where(alarm_events.c.ts <= end_date)
            .order_by(alarm_events.c.ts.desc())
        ).mappings().all()
        return [dict(r) for r in rows]

def clear_alarm_events():
    """Delete all alarm events."""
    with init_engine().begin() as con:
        res = con.execute(delete(alarm_events))
        return res.rowcount

def clear_report_data():
    """Delete all tag values (report data)."""
    with init_engine().begin() as con:
        res = con.execute(delete(tag_values))
        return res.rowcount

def clear_report_data_by_logger(logger_id: int):
    """Delete tag values for specific logger."""
    with init_engine().begin() as con:
        # Get tag IDs for this logger
        tag_ids_result = con.execute(
            select(data_logger_tags.c.tag_id)
            .where(data_logger_tags.c.logger_id == logger_id)
        ).mappings().all()
        
        if not tag_ids_result:
            return 0
            
        tag_ids = [r["tag_id"] for r in tag_ids_result]
        res = con.execute(delete(tag_values).where(tag_values.c.tag_id.in_(tag_ids)))
        return res.rowcount
# Delete a single alarm event by id
def delete_alarm_event_row(eid: int) -> int:
    with init_engine().begin() as con:
        res = con.execute(delete(alarm_events).where(alarm_events.c.id == eid))
        return res.rowcount
def delete_alarm_events(ids: list[int]) -> int:
    with init_engine().begin() as con:
        res = con.execute(delete(alarm_events).where(alarm_events.c.id.in_(ids)))
        return res.rowcount 
    
def list_alarm_report():
    """Ghép INCOMING với OUTGOING + rule info để làm báo cáo alarm."""
    items = []
    with init_engine().connect() as con:
        # Lấy tất cả event INCOMING
        q_in = select(
            alarm_events.c.id,
            alarm_events.c.ts.label("incoming_date"),
            alarm_events.c.value.label("incoming_value"),
            alarm_events.c.target,
            alarm_events.c.name,
            alarm_events.c.level,
            alarm_events.c.note
        ).where(alarm_events.c.note.like("Alarm INCOMING%"))

        incoming_rows = con.execute(q_in).mappings().all()

        for inc in incoming_rows:
            # Tìm OUTGOING sau INCOMING đó
            q_out = select(
                alarm_events.c.ts.label("outgoing_date"),
                alarm_events.c.value.label("outgoing_value")
            ).where(
                and_(
                    alarm_events.c.target == inc["target"],
                    alarm_events.c.note.like("Alarm OUTCOME%"),
                    alarm_events.c.ts > inc["incoming_date"]
                )
            ).order_by(alarm_events.c.ts.asc()).limit(1)

            out = con.execute(q_out).mappings().first()

            # Lấy rule để biết code / operator / threshold
            q_rule = select(
                alarm_rules.c.code,
                alarm_rules.c.operator,
                alarm_rules.c.threshold
            ).where(alarm_rules.c.target == inc["target"])
            rule = con.execute(q_rule).mappings().first()

            items.append({
                "acknowledged": False,  # sau này bạn có thể thêm cột ack
                "code": rule["code"] if rule else "",
                "level": inc["level"],
                "incoming_date": inc["incoming_date"].strftime("%Y-%m-%d %H:%M:%S"),
                "incoming_value": inc["incoming_value"],
                "outgoing_date": out["outgoing_date"].strftime("%Y-%m-%d %H:%M:%S") if out else "",
                "outgoing_value": out["outgoing_value"] if out else "",
                "operator": rule["operator"] if rule else "",
                "threshold": rule["threshold"] if rule else ""
            })

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

def get_data_logger_tag_ids(lid: int) -> list[int]:
    with init_engine().connect() as con:
        rows = con.execute(select(data_logger_tags.c.tag_id).where(data_logger_tags.c.logger_id == lid)).all()
        return [r[0] for r in rows]

# Alias for backward compatibility
def list_data_logger_tags(lid: int) -> list[int]:
    """Alias for get_data_logger_tag_ids"""
    return get_data_logger_tag_ids(lid)

def add_data_logger_row(data: dict, tag_ids: list[int]) -> int:
    with init_engine().begin() as con:
        res = con.execute(insert(data_loggers).values(**data))
        new_id = res.inserted_primary_key[0]
        if tag_ids:
            con.execute(
                data_logger_tags.insert(),
                [{"logger_id": new_id, "tag_id": tid} for tid in tag_ids]
            )
        return new_id

def update_data_logger_row(lid: int, data: dict, tag_ids: list[int]) -> int:
    """Cập nhật logger và thay toàn bộ tập tag đính kèm."""
    with init_engine().begin() as con:
        res = con.execute(update(data_loggers).where(data_loggers.c.id == lid).values(**data))
        con.execute(delete(data_logger_tags).where(data_logger_tags.c.logger_id == lid))
        if tag_ids:
            con.execute(
                data_logger_tags.insert(),
                [{"logger_id": lid, "tag_id": tid} for tid in tag_ids]
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
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
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

def get_latest_tag_values_batch(tag_ids: list[int]) -> dict:
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
        
def add_subdashboard_row(data: dict, tag_ids: list[int] = None) -> int:
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
    
def get_subdashboard_tags(sid: int):
    """
    Trả về danh sách tag (dict) thuộc subdashboard (dashboard) có id=sid.
    Chỉ lấy thông tin tag cơ bản, value và timestamp sẽ được update qua realtime.
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
            # Set default values - will be updated by realtime updates
            tag_dict['value'] = None
            tag_dict['ts'] = "--:--"
            tag_dict['alarm_status'] = "Normal"
            result.append(tag_dict)
        
        return result
    
def list_subdash_groups():
    with init_engine().connect() as con:
        return con.execute(select(subdash_tag_groups)).mappings().all()

def list_subdash_groups_for_dashboard(dashboard_id: int):
    """Get all groups for a specific subdashboard."""
    with init_engine().connect() as con:
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
        ).mappings().all()
        
        result = []
        for r in rows:
            tag_dict = dict(r)
            # Get latest value and timestamp for each tag
            value, ts = get_latest_tag_value(tag_dict['id'])
            print(f"Tag {tag_dict['name']} latest value: {value} at {ts}")
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

def get_logger_tag_ids(logger_id: int) -> list[int]:
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
            query = select(
                data_logger_tags.c.tag_id,
                data_loggers.c.id.label("logger_id"),
                data_loggers.c.name.label("logger_name"),
                data_loggers.c.interval_sec,
                data_loggers.c.enabled
            ).select_from(
                data_logger_tags.join(data_loggers, data_logger_tags.c.logger_id == data_loggers.c.id)
            )
            
            if device_id:
                # Filter by device if specified
                query = query.select_from(
                    data_logger_tags
                    .join(data_loggers, data_logger_tags.c.logger_id == data_loggers.c.id)
                    .join(tags, data_logger_tags.c.tag_id == tags.c.id)
                ).where(tags.c.device_id == device_id)
            
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

def batch_insert_data_logs(log_entries: list[dict]):
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