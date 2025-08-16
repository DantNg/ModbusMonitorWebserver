# Delete a single alarm event by id
def delete_alarm_event_row(eid: int) -> int:
    with init_engine().begin() as con:
        res = con.execute(delete(alarm_events).where(alarm_events.c.id == eid))
        return res.rowcount
# ----------- ALARM EVENTS (history) -----------
def list_alarm_events():
    """Return all alarm events, newest first."""
    with init_engine().connect() as con:
        rows = con.execute(select(alarm_events).order_by(alarm_events.c.ts.desc())).mappings().all()
        return [dict(r) for r in rows]

def clear_alarm_events():
    """Delete all alarm events."""
    with init_engine().begin() as con:
        res = con.execute(delete(alarm_events))
        return res.rowcount
from ast import Dict, stmt
import os
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float, Boolean,
    DateTime, Enum, ForeignKey, select, insert, update, delete, func,cast,and_, asc
)
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from datetime import datetime
# ---------- Singleton Engine ----------
_engine: Engine | None = None
_md = MetaData()

def init_engine():
    """Khởi tạo engine một lần (singleton)."""
    global _engine
    if _engine is not None:
        return _engine
    load_dotenv()
    uri = os.getenv("MYSQL_URI", "mysql+pymysql://root:123456@localhost:3306/modbus_monitor_db")
    pool_size = int(os.getenv("POOL_SIZE", "8"))
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
    Column("datatype", Enum("Word","DWord","Float","Bit"), default="Word"),
    Column("unit", String(20)),
    Column("scale", Float, default=1.0),
    Column("offset", Float, default=0.0),
    Column("grp", String(50), default="Group1"),
    Column("description", String(255)),
)

tag_values = Table(
    "tag_values", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("tag_id", Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False, index=True),
    Column("ts", DateTime, nullable=False),
    Column("value", Float, nullable=False),
)

alarm_rules = Table(
    "alarm_rules", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("enabled", Boolean, default=True),
    Column("code", String(50)),
    Column("name", String(120)),
    Column("level", Enum("Low", "Medium", "High", "Critical"), default="High"),
    Column("target", String(120)),          # ví dụ: "device_id.tag_id" hoặc "device_name.tag_name"
    Column("operator", String(30)),         # >, <, >=, <=, ==, !=
    Column("threshold", String(64)),        # số hoặc "min,max"
    Column("on_stable_sec", Integer, default=0),
    Column("off_stable_sec", Integer, default=0),
    Column("created_at", DateTime, server_default=func.now()),
)
# (tuỳ chọn) Báo động tối giản – chỉ để có bảng lịch sử
alarm_events = Table(
    "alarm_events", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts", DateTime, nullable=False),
    Column("name", String(120)),
    Column("level", String(20)),
    Column("target", String(120)),
    Column("value", Float),
    Column("note", String(255)),
)

# --- Bảng Data Logger ---
data_loggers = Table(
    "data_loggers", _md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(120), nullable=False),
    Column("interval_sec", Integer, nullable=False, default=60),
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
def create_schema():
    """Tạo bảng nếu chưa có (idempotent)."""
    engine = init_engine()
    _md.create_all(engine)

# ---------- CRUD NHANH (dùng trực tiếp trong route/service) ----------
def list_devices():
    with init_engine().connect() as con:
        rows = con.execute(select(devices)).mappings().all()
        return [dict(r) for r in rows]

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
    """rows = [(tag_id, ts, value), ...]"""
    if not rows:
        return 0
    with init_engine().begin() as con:
        con.execute(
            tag_values.insert(),
            [{"tag_id": t, "ts": ts, "value": v} for (t, ts, v) in rows]
        )
        return len(rows)
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
                alarm_rules.c.target,             # tag_id (string)
                alarm_rules.c.operator,
                alarm_rules.c.threshold,
                alarm_rules.c.on_stable_sec,
                alarm_rules.c.off_stable_sec,
                alarm_rules.c.enabled,
                alarm_rules.c.level,
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
 
def insert_alarm_event(ts, name, level, target, value, note=""):
    with init_engine().begin() as con:
        con.execute(
            alarm_events.insert().values(
                ts=ts,
                name=name,
                level=level,
                target=target,
                value=value,
                note=note
            )
        )
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

def get_latest_tag_value(tag_id: int):
    with init_engine().connect() as con:
        row = con.execute(
            select(tag_values.c.value, tag_values.c.ts)
            .where(tag_values.c.tag_id == tag_id)
            .order_by(tag_values.c.ts.desc())
            .limit(1)
        ).first()
        if row:
            return row.value, row.ts
        return None, None
    
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

