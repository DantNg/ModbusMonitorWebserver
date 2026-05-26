"""
alarm_strategies.py
--------------------
Factory + Strategy design pattern cho qtag card alarm evaluation.

Kiến trúc:
    CardAlarmStrategy (ABC)
        ├── Qtag6Strategy
        ├── Qtag4Strategy
        ├── Qtag3Strategy
        ├── Qtag2Strategy
        ├── Single3Strategy
        ├── PVOnlyStrategy
        ├── PVDualStrategy
        └── QuadCardStrategy

    AlarmStrategyFactory
        └── get(card_type)      → CardAlarmStrategy | None
        └── register(strategy)  → thêm loại mới không cần sửa code cũ
        └── all_strategies()    → duyệt tất cả để restore khi khởi động

Cách thêm loại qtag mới:
    1. Tạo class kế thừa CardAlarmStrategy
    2. Implement tất cả abstract methods
    3. Thêm vào danh sách _BUILTIN_STRATEGIES ở cuối file
       (hoặc gọi AlarmStrategyFactory.register() tại runtime)
"""

from abc import ABC, abstractmethod
from typing import Optional, List, NamedTuple


# ---------------------------------------------------------------------------
# ColumnDef – mô tả một cột cần evaluate của card
# ---------------------------------------------------------------------------

class ColumnDef(NamedTuple):
    """Định nghĩa một cột (left / right) của card cần được evaluate alarm.

    Attributes:
        name:       "left" hoặc "right"
        pv_tag_id:  Tag ID dùng để so sánh với ngưỡng alarm (Process Value)
        sv_tag_id:  Tag ID phụ – chỉ hiển thị, KHÔNG dùng cho logic alarm
                    (None với hầu hết các loại card, tag3/tag4 với QuadCard)
    """
    name: str
    pv_tag_id: int
    sv_tag_id: Optional[int] = None


# ---------------------------------------------------------------------------
# Abstract base – Strategy interface
# ---------------------------------------------------------------------------

class CardAlarmStrategy(ABC):
    """Interface mà mọi qtag alarm strategy phải implement.

    AlarmWorker dùng interface này để evaluate alarm mà KHÔNG cần biết
    loại card cụ thể là gì.  Mọi sự khác biệt giữa các loại (cách lấy
    tag IDs, format event, DB methods, tên hiển thị...) được đóng gói
    hoàn toàn trong concrete strategy tương ứng.
    """

    # ── Identity ──────────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def card_type(self) -> str:
        """Chuỗi định danh duy nhất cho loại card này (vd: 'qtag6', 'quad')."""
        ...

    @property
    @abstractmethod
    def socket_event_name(self) -> str:
        """Tên Socket.IO event phát ra khi có alarm (INCOMING / OUTGOING)."""
        ...

    @property
    def device_label(self) -> str:
        """Nhãn thiết bị dùng trong nội dung email/SMS thông báo."""
        return "Tag Card"

    # ── Column layout ─────────────────────────────────────────────────────────

    @abstractmethod
    def get_columns(self, card_info: dict) -> List[ColumnDef]:
        """Trả về danh sách ColumnDef cần evaluate dựa trên card_info từ DB.

        Single-column card → list 1 phần tử.
        Dual-column card   → list 2 phần tử (left, right).
        """
        ...

    def get_managed_tag_ids(self, card_info: dict) -> List[int]:
        """Tất cả tag ID (PV + SV) thuộc card này.

        AlarmWorker dùng danh sách này để bỏ qua các tag đã được card
        alarm quản lý, tránh duplicate notification với simple alarm rules.
        """
        ids: List[int] = []
        for col in self.get_columns(card_info):
            if col.pv_tag_id:
                ids.append(col.pv_tag_id)
            if col.sv_tag_id:
                ids.append(col.sv_tag_id)
        return ids

    # ── Card info from DB ─────────────────────────────────────────────────────

    @abstractmethod
    def fetch_card_info(self, db, card_id: int) -> Optional[dict]:
        """Lấy thông tin card từ database theo card_id.

        Mỗi loại card có thể dùng DB method khác nhau nên được đặt ở đây.
        """
        ...

    # ── Display names ─────────────────────────────────────────────────────────

    def get_display_name(self, card_id: int, card_info: dict) -> str:
        """Tên hiển thị của card trong log / thông báo alarm."""
        return card_info.get("card_title", f"{self.card_type} {card_id}")

    def get_column_display(self, column: str, card_info: dict) -> str:
        """Nhãn cột (Left / Right) dùng trong ghi chú alarm.
        Trả về empty string nếu card không có custom title, tránh hiển thị 'Left'/'Right' generic.
        """
        key = "left_title" if column == "left" else "right_title"
        title = card_info.get(key)
        if title and str(title).strip():
            return str(title).strip()
        return ""

    # ── Alarm state key ───────────────────────────────────────────────────────

    def make_alarm_key(self, card_id: int, column: str) -> str:
        """Tạo key dùng trong _alarm_states / _alarm_since dictionaries."""
        return f"card_{self.card_type}_{card_id}_{column}"

    # ── Socket event payload ──────────────────────────────────────────────────

    @abstractmethod
    def build_event(
        self,
        card_id: int,
        column: str,
        alarm_type: str,
        status: str,
        pv_tag_id: int,
        pv_value: float,
        sv_value: Optional[float],
        threshold: float,
        operator: str,
        timestamp: str,
    ) -> dict:
        """Tạo dict payload cho Socket.IO event (INCOMING hoặc OUTGOING)."""
        ...

    # ── Database persistence ──────────────────────────────────────────────────

    @abstractmethod
    def save_alarm_state(
        self,
        db,
        card_id: int,
        column: str,
        alarm_type: str,
        pv_value: float,
        sv_value: Optional[float],
        threshold: float,
        operator: str,
    ) -> Optional[int]:
        """Lưu trạng thái alarm vào DB khi INCOMING.  Trả về ID record mới."""
        ...

    @abstractmethod
    def delete_alarm_state(self, db, card_id: int, column: str) -> int:
        """Xóa trạng thái alarm khỏi DB khi OUTGOING.  Trả về số row đã xóa."""
        ...


# ---------------------------------------------------------------------------
# Shared helper – payload chuẩn cho card_alarm_event
# ---------------------------------------------------------------------------

def _card_event(
    card_type: str,
    card_id: int,
    column: str,
    alarm_type: str,
    status: str,
    pv_tag_id: int,
    pv_value: float,
    threshold: float,
    operator: str,
    timestamp: str,
) -> dict:
    """Tạo payload chuẩn cho sự kiện card_alarm_event (dùng chung cho hầu hết loại card)."""
    return {
        "card_type": card_type,
        "card_id": card_id,
        "column": column,
        "alarm_type": alarm_type,
        "status": status,
        "tag_id": pv_tag_id,
        "pv_value": pv_value,
        "threshold": threshold,
        "operator": operator,
        "timestamp": timestamp,
    }


# ---------------------------------------------------------------------------
# Mixin dùng chung cho các card lưu trạng thái vào bảng card_alarm_states
# ---------------------------------------------------------------------------

class _CardAlarmStateMixin:
    """Mixin cung cấp DB methods chuẩn cho các loại card dùng bảng card_alarm_states."""

    def save_alarm_state(self, db, card_id, column, alarm_type, pv_value, sv_value, threshold, operator):
        return db.insert_card_alarm_state(
            card_type=self.card_type,
            card_id=card_id,
            column=column,
            alarm_type=alarm_type,
            pv_value=pv_value,
            threshold=threshold,
            operator=operator,
        )

    def delete_alarm_state(self, db, card_id, column):
        return db.delete_card_alarm_state(self.card_type, card_id, column)

    def fetch_card_info(self, db, card_id: int):
        return db.get_card_info_for_alarm(self.card_type, card_id)


# ---------------------------------------------------------------------------
# Concrete strategies – Card types
# ---------------------------------------------------------------------------

class Qtag6Strategy(_CardAlarmStateMixin, CardAlarmStrategy):
    """Qtag6: dual-column, tag1_id = left PV, tag2_id = right PV."""

    @property
    def card_type(self) -> str:
        return "qtag6"

    @property
    def socket_event_name(self) -> str:
        return "card_alarm_event"

    def get_columns(self, card_info: dict) -> List[ColumnDef]:
        cols = []
        if card_info.get("tag1_id"):
            cols.append(ColumnDef("left", int(card_info["tag1_id"])))
        if card_info.get("tag2_id"):
            cols.append(ColumnDef("right", int(card_info["tag2_id"])))
        return cols

    def build_event(self, card_id, column, alarm_type, status, pv_tag_id, pv_value, sv_value, threshold, operator, timestamp):
        return _card_event(self.card_type, card_id, column, alarm_type, status, pv_tag_id, pv_value, threshold, operator, timestamp)


class Qtag4Strategy(_CardAlarmStateMixin, CardAlarmStrategy):
    """Qtag4: dual-column, tag1_id = left PV, tag2_id = right PV."""

    @property
    def card_type(self) -> str:
        return "qtag4"

    @property
    def socket_event_name(self) -> str:
        return "card_alarm_event"

    def get_columns(self, card_info: dict) -> List[ColumnDef]:
        cols = []
        if card_info.get("tag1_id"):
            cols.append(ColumnDef("left", int(card_info["tag1_id"])))
        if card_info.get("tag2_id"):
            cols.append(ColumnDef("right", int(card_info["tag2_id"])))
        return cols

    def build_event(self, card_id, column, alarm_type, status, pv_tag_id, pv_value, sv_value, threshold, operator, timestamp):
        return _card_event(self.card_type, card_id, column, alarm_type, status, pv_tag_id, pv_value, threshold, operator, timestamp)


class Qtag3Strategy(_CardAlarmStateMixin, CardAlarmStrategy):
    """Qtag3: single-column, tag1_id = left PV."""

    @property
    def card_type(self) -> str:
        return "qtag3"

    @property
    def socket_event_name(self) -> str:
        return "card_alarm_event"

    def get_columns(self, card_info: dict) -> List[ColumnDef]:
        tid = card_info.get("tag1_id")
        return [ColumnDef("left", int(tid))] if tid else []

    def get_column_display(self, column: str, card_info: dict) -> str:
        # Single-column card: avoid generic "Left" label in notifications.
        return (card_info.get("column_title") or "").strip()

    def build_event(self, card_id, column, alarm_type, status, pv_tag_id, pv_value, sv_value, threshold, operator, timestamp):
        return _card_event(self.card_type, card_id, column, alarm_type, status, pv_tag_id, pv_value, threshold, operator, timestamp)


class Qtag2Strategy(_CardAlarmStateMixin, CardAlarmStrategy):
    """Qtag2: single-column, tag1_id = left PV."""

    @property
    def card_type(self) -> str:
        return "qtag2"

    @property
    def socket_event_name(self) -> str:
        return "card_alarm_event"

    def get_columns(self, card_info: dict) -> List[ColumnDef]:
        tid = card_info.get("tag1_id")
        return [ColumnDef("left", int(tid))] if tid else []

    def get_column_display(self, column: str, card_info: dict) -> str:
        # Single-column card: avoid generic "Left" label in notifications.
        return (card_info.get("column_title") or "").strip()

    def build_event(self, card_id, column, alarm_type, status, pv_tag_id, pv_value, sv_value, threshold, operator, timestamp):
        return _card_event(self.card_type, card_id, column, alarm_type, status, pv_tag_id, pv_value, threshold, operator, timestamp)


class Single3Strategy(_CardAlarmStateMixin, CardAlarmStrategy):
    """Single3: single-column, pv_tag_id = left PV."""

    @property
    def card_type(self) -> str:
        return "single3"

    @property
    def socket_event_name(self) -> str:
        return "card_alarm_event"

    def get_columns(self, card_info: dict) -> List[ColumnDef]:
        tid = card_info.get("pv_tag_id")
        return [ColumnDef("left", int(tid))] if tid else []

    def get_column_display(self, column: str, card_info: dict) -> str:
        # Single-column card: do not prepend "Left" in alarm message.
        return ""

    def build_event(self, card_id, column, alarm_type, status, pv_tag_id, pv_value, sv_value, threshold, operator, timestamp):
        return _card_event(self.card_type, card_id, column, alarm_type, status, pv_tag_id, pv_value, threshold, operator, timestamp)


class PVOnlyStrategy(_CardAlarmStateMixin, CardAlarmStrategy):
    """PV Only: single-column, pv_tag_id = left PV."""

    @property
    def card_type(self) -> str:
        return "pv_only"

    @property
    def socket_event_name(self) -> str:
        return "card_alarm_event"

    def get_columns(self, card_info: dict) -> List[ColumnDef]:
        tid = card_info.get("pv_tag_id")
        return [ColumnDef("left", int(tid))] if tid else []

    def get_column_display(self, column: str, card_info: dict) -> str:
        # Single-column card: do not prepend "Left" in alarm message.
        return ""

    def build_event(self, card_id, column, alarm_type, status, pv_tag_id, pv_value, sv_value, threshold, operator, timestamp):
        return _card_event(self.card_type, card_id, column, alarm_type, status, pv_tag_id, pv_value, threshold, operator, timestamp)


class PVDualStrategy(_CardAlarmStateMixin, CardAlarmStrategy):
    """PV Dual: dual-column, left_tag_id = left PV, right_tag_id = right PV."""

    @property
    def card_type(self) -> str:
        return "pv_dual"

    @property
    def socket_event_name(self) -> str:
        return "card_alarm_event"

    def get_columns(self, card_info: dict) -> List[ColumnDef]:
        cols = []
        if card_info.get("left_tag_id"):
            cols.append(ColumnDef("left", int(card_info["left_tag_id"])))
        if card_info.get("right_tag_id"):
            cols.append(ColumnDef("right", int(card_info["right_tag_id"])))
        return cols

    def build_event(self, card_id, column, alarm_type, status, pv_tag_id, pv_value, sv_value, threshold, operator, timestamp):
        return _card_event(self.card_type, card_id, column, alarm_type, status, pv_tag_id, pv_value, threshold, operator, timestamp)


# ---------------------------------------------------------------------------
# Concrete strategy – Quad Card (layout và DB khác các loại card thông thường)
# ---------------------------------------------------------------------------

class QuadCardStrategy(CardAlarmStrategy):
    """Quad Card: dual-column với PV + SV tag cho mỗi cột.

    Điểm khác biệt so với card thông thường:
    - alarm_key prefix: 'quad_'  (thay vì 'card_')
    - Socket event:     'quad_alarm_event'  (payload gồm tag1_value + tag2_value)
    - DB persistence:   quad_alarm_states table  (không phải card_alarm_states)
    - Column layout:    left = (tag1_id=PV, tag3_id=SV), right = (tag2_id=PV, tag4_id=SV)
    - Managed tag IDs:  cả 4 tag (tag1–tag4) đều thuộc card này
    """

    @property
    def card_type(self) -> str:
        return "quad"

    @property
    def socket_event_name(self) -> str:
        return "quad_alarm_event"

    @property
    def device_label(self) -> str:
        return "Quad Tag Card"

    def get_columns(self, card_info: dict) -> List[ColumnDef]:
        """
        Left column:  PV = tag1_id, SV = tag3_id
        Right column: PV = tag2_id, SV = tag4_id
        """
        cols = []
        if card_info.get("tag1_id"):
            sv_id = int(card_info["tag3_id"]) if card_info.get("tag3_id") else None
            cols.append(ColumnDef("left", int(card_info["tag1_id"]), sv_id))
        if card_info.get("tag2_id"):
            sv_id = int(card_info["tag4_id"]) if card_info.get("tag4_id") else None
            cols.append(ColumnDef("right", int(card_info["tag2_id"]), sv_id))
        return cols

    def get_managed_tag_ids(self, card_info: dict) -> List[int]:
        """Gộp cả 4 tag (PV + SV cả hai cột) để tránh duplicate alarm."""
        ids: List[int] = []
        for key in ("tag1_id", "tag2_id", "tag3_id", "tag4_id"):
            if card_info.get(key):
                ids.append(int(card_info[key]))
        return ids

    def fetch_card_info(self, db, card_id: int) -> Optional[dict]:
        return db.get_quad_card_by_id(card_id)

    def get_display_name(self, card_id: int, card_info: dict) -> str:
        return card_info.get("card_title", card_info.get("name", f"Quad {card_id}"))

    def get_column_display(self, column: str, card_info: dict) -> str:
        key = "left_title" if column == "left" else "right_title"
        return card_info.get(key, "Column Left" if column == "left" else "Column Right")

    def make_alarm_key(self, card_id: int, column: str) -> str:
        return f"quad_{card_id}_{column}"

    def build_event(
        self,
        card_id: int,
        column: str,
        alarm_type: str,
        status: str,
        pv_tag_id: int,
        pv_value: float,
        sv_value: Optional[float],
        threshold: float,
        operator: str,
        timestamp: str,
    ) -> dict:
        """Payload cho quad_alarm_event bao gồm cả tag1_value (PV) và tag2_value (SV)."""
        return {
            "quad_id": card_id,
            "column": column,
            "alarm_type": alarm_type,
            "status": status,
            "tag_id": pv_tag_id,
            "tag1_value": pv_value,
            "tag2_value": sv_value,
            "threshold": threshold,
            "operator": operator,
            "timestamp": timestamp,
        }

    def save_alarm_state(
        self,
        db,
        card_id: int,
        column: str,
        alarm_type: str,
        pv_value: float,
        sv_value: Optional[float],
        threshold: float,
        operator: str,
    ) -> Optional[int]:
        return db.insert_quad_alarm_state(
            quad_id=card_id,
            column=column,
            alarm_type=alarm_type,
            tag1_value=pv_value,
            tag2_value=sv_value,
            threshold=threshold,
            operator=operator,
        )

    def delete_alarm_state(self, db, card_id: int, column: str) -> int:
        return db.delete_quad_alarm_state(card_id, column)


# ---------------------------------------------------------------------------
# AlarmStrategyFactory
# ---------------------------------------------------------------------------

# Danh sách các strategy có sẵn – thêm loại mới vào đây là đủ
_BUILTIN_STRATEGIES: List[CardAlarmStrategy] = [
    Qtag6Strategy(),
    Qtag4Strategy(),
    Qtag3Strategy(),
    Qtag2Strategy(),
    Single3Strategy(),
    PVOnlyStrategy(),
    PVDualStrategy(),
    QuadCardStrategy(),
]


class AlarmStrategyFactory:
    """Factory tra cứu CardAlarmStrategy theo card_type string.

    Sử dụng:
        strategy = AlarmStrategyFactory.get("qtag6")   # → Qtag6Strategy
        strategy = AlarmStrategyFactory.get("quad")    # → QuadCardStrategy
        strategy = AlarmStrategyFactory.get("unknown") # → None

    Mở rộng (không cần sửa file này):
        AlarmStrategyFactory.register(MyNewStrategy())
    """

    _registry: dict = {}
    _initialized: bool = False

    @classmethod
    def _ensure_initialized(cls) -> None:
        """Khởi tạo registry lần đầu (lazy init để tránh side effect khi import)."""
        if not cls._initialized:
            for strategy in _BUILTIN_STRATEGIES:
                cls._registry[strategy.card_type] = strategy
            cls._initialized = True

    @classmethod
    def get(cls, card_type: str) -> Optional[CardAlarmStrategy]:
        """Trả về strategy tương ứng hoặc None nếu chưa được đăng ký."""
        cls._ensure_initialized()
        return cls._registry.get(card_type)

    @classmethod
    def register(cls, strategy: CardAlarmStrategy) -> None:
        """Đăng ký strategy tùy chỉnh.  Có thể override strategy có sẵn nếu cần."""
        cls._ensure_initialized()
        cls._registry[strategy.card_type] = strategy

    @classmethod
    def all_strategies(cls) -> List[CardAlarmStrategy]:
        """Trả về toàn bộ strategy đã đăng ký (dùng khi restore state on startup)."""
        cls._ensure_initialized()
        return list(cls._registry.values())

    @classmethod
    def registered_types(cls) -> List[str]:
        """Trả về danh sách card_type đã đăng ký (dùng để debug/log)."""
        cls._ensure_initialized()
        return list(cls._registry.keys())
