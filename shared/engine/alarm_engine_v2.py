"""
Generic Alarm Engine (v2)
--------------------------
Pure Python — không có DB, không có SocketIO, không có Flask.
Card-type-agnostic: không import bất kỳ logic nào từ alarm_strategies.py.

Cấu trúc:
    Signal          — trạng thái đầu vào (tag_id → value/timestamp)
    AlarmState      — enum trạng thái hiện tại của một alarm rule
    AlarmEvent      — event được emit khi trạng thái thay đổi
    Condition       — ABC + 2 implementations (StaticThreshold, TagCompare)
    GenericAlarmRule — bộ tứ (conditions, severity, stable_on_sec, stable_off_sec)
    StabilityTracker — debounce logic (on/off stable seconds)
    AlarmEvaluator  — vòng lặp evaluate toàn bộ rule set

Thiết kế:
    - AlarmEvaluator không biết card_type — chỉ biết tags và conditions
    - Caller (alarm_worker.py) chịu trách nhiệm push events → Socket.IO và DB
    - card_alarm_translator.py chịu trách nhiệm map DB rows → GenericAlarmRule
"""

from __future__ import annotations

import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Signal — đầu vào của engine
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Signal:
    """
    Snapshot giá trị một tag tại một thời điểm.

    Attributes:
        tag_id      — ID trong bảng `tags`
        value       — giá trị đã apply scale + offset; None nếu không đọc được
        timestamp   — UNIX epoch (seconds); default = thời điểm tạo
    """
    tag_id: int
    value: Optional[float]
    timestamp: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# AlarmState — trạng thái alarm
# ---------------------------------------------------------------------------

class AlarmState(Enum):
    """
    Trạng thái của một alarm rule sau evaluate.

        NORMAL      — điều kiện alarm KHÔNG thoả mãn (hoặc chưa đủ stable_off_sec)
        ALARM       — điều kiện alarm ĐANG thoả mãn (đã qua stable_on_sec)
        PENDING_ON  — điều kiện thoả mãn nhưng chưa đủ stable_on_sec
        PENDING_OFF — điều kiện không còn thoả nhưng chưa đủ stable_off_sec
    """
    NORMAL = "normal"
    ALARM = "alarm"
    PENDING_ON = "pending_on"
    PENDING_OFF = "pending_off"


# ---------------------------------------------------------------------------
# AlarmEvent — output của engine
# ---------------------------------------------------------------------------

@dataclass
class AlarmEvent:
    """
    Event được tạo khi trạng thái alarm thay đổi.
    Caller có thể persist vào DB hoặc push lên Socket.IO.

    Attributes:
        rule_id         — ID của rule phát sinh event
        rule_label      — nhãn mô tả rule (để log/notify)
        from_state      — trạng thái trước
        to_state        — trạng thái sau
        trigger_value   — giá trị tag gây ra transition
        trigger_tag_id  — tag nào gây ra transition
        timestamp       — thời điểm transition xảy ra
        extra           — metadata tuỳ ý (card_id, card_type, ...)
    """
    rule_id: str
    rule_label: str
    from_state: AlarmState
    to_state: AlarmState
    trigger_value: Optional[float]
    trigger_tag_id: Optional[int]
    timestamp: float = field(default_factory=time.time)
    extra: dict = field(default_factory=dict)

    @property
    def is_alarm_on(self) -> bool:
        """Chuyển sang trạng thái ALARM."""
        return self.to_state == AlarmState.ALARM

    @property
    def is_alarm_off(self) -> bool:
        """Chuyển từ ALARM sang NORMAL."""
        return self.from_state == AlarmState.ALARM and self.to_state == AlarmState.NORMAL


# ---------------------------------------------------------------------------
# Condition — ABC
# ---------------------------------------------------------------------------

class Condition(ABC):
    """
    ABC cho tất cả loại điều kiện alarm.
    Mỗi Condition chỉ biết cách kiểm tra xem điều kiện có thoả mãn không.
    """

    @abstractmethod
    def evaluate(self, signal_map: Dict[int, Signal]) -> Tuple[bool, Optional[int], Optional[float]]:
        """
        Kiểm tra điều kiện dựa trên signal_map.

        Returns:
            (satisfied, trigger_tag_id, trigger_value)
                satisfied      — True nếu điều kiện thoả mãn
                trigger_tag_id — tag_id đã gây ra result (None nếu không xác định)
                trigger_value  — giá trị tag đó tại thời điểm evaluate
        """

    @abstractmethod
    def __repr__(self) -> str:
        pass


# ---------------------------------------------------------------------------
# StaticThresholdCondition — so sánh tag với ngưỡng cố định
# ---------------------------------------------------------------------------

# Map operator string → callable comparison
_OP_MAP = {
    ">":  lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<":  lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


class StaticThresholdCondition(Condition):
    """
    Điều kiện: value của tag_id {operator} threshold.

    Ví dụ:
        tag_id=10, operator=">", threshold=80.0
        → True nếu tag[10].value > 80.0

    Attributes:
        tag_id    — tag cần monitor
        operator  — một trong: ">" ">=" "<" "<=" "==" "!="
        threshold — ngưỡng so sánh (float)
    """

    VALID_OPERATORS = frozenset(_OP_MAP.keys())

    def __init__(self, tag_id: int, operator: str, threshold: float):
        if operator not in self.VALID_OPERATORS:
            raise ValueError(f"Operator không hợp lệ: '{operator}'. "
                             f"Phải là một trong: {sorted(self.VALID_OPERATORS)}")
        self.tag_id = tag_id
        self.operator = operator
        self.threshold = float(threshold)
        self._op_fn = _OP_MAP[operator]

    def evaluate(
        self, signal_map: Dict[int, Signal]
    ) -> Tuple[bool, Optional[int], Optional[float]]:
        sig = signal_map.get(self.tag_id)
        if sig is None or sig.value is None:
            return (False, self.tag_id, None)
        result = self._op_fn(sig.value, self.threshold)
        return (result, self.tag_id, sig.value)

    def __repr__(self):
        return (f"StaticThreshold(tag={self.tag_id} "
                f"{self.operator} {self.threshold})")


# ---------------------------------------------------------------------------
# TagCompareCondition — so sánh 2 tags với nhau
# ---------------------------------------------------------------------------

class TagCompareCondition(Condition):
    """
    Điều kiện: value[tag_a] {operator} value[tag_b].

    Ví dụ:
        tag_a_id=10, operator=">", tag_b_id=11
        → True nếu tag[10].value > tag[11].value

    Attributes:
        tag_a_id  — tag bên trái
        operator  — một trong: ">" ">=" "<" "<=" "==" "!="
        tag_b_id  — tag bên phải
    """

    VALID_OPERATORS = frozenset(_OP_MAP.keys())

    def __init__(self, tag_a_id: int, operator: str, tag_b_id: int):
        if operator not in self.VALID_OPERATORS:
            raise ValueError(f"Operator không hợp lệ: '{operator}'.")
        self.tag_a_id = tag_a_id
        self.operator = operator
        self.tag_b_id = tag_b_id
        self._op_fn = _OP_MAP[operator]

    def evaluate(
        self, signal_map: Dict[int, Signal]
    ) -> Tuple[bool, Optional[int], Optional[float]]:
        sig_a = signal_map.get(self.tag_a_id)
        sig_b = signal_map.get(self.tag_b_id)
        if sig_a is None or sig_a.value is None:
            return (False, self.tag_a_id, None)
        if sig_b is None or sig_b.value is None:
            return (False, self.tag_b_id, None)
        result = self._op_fn(sig_a.value, sig_b.value)
        # trigger_tag_id: tag nào vi phạm ngưỡng — ở đây là tag_a
        return (result, self.tag_a_id, sig_a.value)

    def __repr__(self):
        return (f"TagCompare(tag={self.tag_a_id} "
                f"{self.operator} tag={self.tag_b_id})")


# ---------------------------------------------------------------------------
# GenericAlarmRule — một rule đầy đủ
# ---------------------------------------------------------------------------

@dataclass
class GenericAlarmRule:
    """
    Một alarm rule đầy đủ.

    Attributes:
        rule_id          — unique string ID (vd: "card_alarm_3_row_2")
        label            — nhãn mô tả (hiển thị trong log, notification)
        conditions       — list Condition; tất cả phải True thì rule active (AND logic)
        severity         — mức độ: "low", "medium", "high", "critical"
        stable_on_sec    — giây cần giữ True trước khi phát alarm (debounce ON)
        stable_off_sec   — giây cần giữ False trước khi clear alarm (debounce OFF)
        extra            — metadata tuỳ ý để đính kèm vào AlarmEvent
    """
    rule_id: str
    label: str
    conditions: List[Condition]
    severity: str = "medium"
    stable_on_sec: float = 0.0
    stable_off_sec: float = 0.0
    extra: dict = field(default_factory=dict)

    def evaluate_conditions(
        self, signal_map: Dict[int, Signal]
    ) -> Tuple[bool, Optional[int], Optional[float]]:
        """
        Evaluate tất cả conditions (AND logic).
        Trả về (all_satisfied, trigger_tag_id, trigger_value).
        trigger_tag_id là tag đầu tiên gây ra satisfied=True.
        """
        if not self.conditions:
            return (False, None, None)

        last_tag: Optional[int] = None
        last_val: Optional[float] = None

        for cond in self.conditions:
            satisfied, tag_id, val = cond.evaluate(signal_map)
            if not satisfied:
                return (False, tag_id, val)
            last_tag = tag_id
            last_val = val

        return (True, last_tag, last_val)


# ---------------------------------------------------------------------------
# StabilityTracker — debounce logic
# ---------------------------------------------------------------------------

class StabilityTracker:
    """
    Xử lý debounce: stable_on_sec và stable_off_sec.

    Flow:
        - Khi condition True:
            * Nếu off_since đang chạy → cancel
            * Nếu chưa có on_since → bắt đầu đếm
            * Nếu đã đủ stable_on_sec → emit ALARM
        - Khi condition False:
            * Nếu on_since đang chạy → cancel
            * Nếu chưa có off_since → bắt đầu đếm
            * Nếu đã đủ stable_off_sec → emit NORMAL
    """

    def __init__(self, stable_on_sec: float = 0.0, stable_off_sec: float = 0.0):
        self.stable_on_sec = max(0.0, float(stable_on_sec))
        self.stable_off_sec = max(0.0, float(stable_off_sec))
        self._on_since: Optional[float] = None   # thời điểm bắt đầu điều kiện True
        self._off_since: Optional[float] = None  # thời điểm bắt đầu điều kiện False
        self._state: AlarmState = AlarmState.NORMAL

    @property
    def current_state(self) -> AlarmState:
        return self._state

    def update(
        self,
        condition_satisfied: bool,
        now: Optional[float] = None,
    ) -> Optional[AlarmState]:
        """
        Cập nhật tracker với kết quả condition mới.

        Returns:
            AlarmState mới nếu có transition; None nếu không thay đổi.
        """
        if now is None:
            now = time.time()

        old_state = self._state

        if condition_satisfied:
            # Có điều kiện → cancel off timer
            self._off_since = None

            if self._state == AlarmState.ALARM:
                # Đang alarm, không cần làm gì
                return None

            if self._on_since is None:
                self._on_since = now
                if self.stable_on_sec == 0:
                    # Không cần debounce → trigger ngay
                    self._state = AlarmState.ALARM
                else:
                    self._state = AlarmState.PENDING_ON
            else:
                # Đang đếm on
                elapsed = now - self._on_since
                if elapsed >= self.stable_on_sec:
                    self._state = AlarmState.ALARM
                # else: vẫn PENDING_ON

        else:
            # Không có điều kiện → cancel on timer
            self._on_since = None

            if self._state == AlarmState.NORMAL:
                return None

            if self._off_since is None:
                self._off_since = now
                if self.stable_off_sec == 0:
                    # Không cần debounce → clear ngay
                    self._state = AlarmState.NORMAL
                else:
                    self._state = AlarmState.PENDING_OFF
            else:
                elapsed = now - self._off_since
                if elapsed >= self.stable_off_sec:
                    self._state = AlarmState.NORMAL
                    self._off_since = None
                # else: vẫn PENDING_OFF

        new_state = self._state
        if new_state != old_state:
            return new_state
        return None

    def reset(self):
        """Reset về NORMAL, xóa hết timers."""
        self._on_since = None
        self._off_since = None
        self._state = AlarmState.NORMAL


# ---------------------------------------------------------------------------
# AlarmEvaluator — engine chính
# ---------------------------------------------------------------------------

class AlarmEvaluator:
    """
    Evaluate toàn bộ tập rule trên một snapshot signals.

    Sử dụng:
        evaluator = AlarmEvaluator()
        evaluator.set_rules(rules_list)  # List[GenericAlarmRule]

        # Mỗi chu kỳ:
        signal_map = {10: Signal(10, 85.3), 11: Signal(11, 40.0)}
        events = evaluator.evaluate(signal_map)

        for event in events:
            if event.is_alarm_on:
                send_notification(event)
            persist_event(event)

    Thread-safety:
        AlarmEvaluator KHÔNG thread-safe. Mỗi worker process nên dùng
        một instance riêng.
    """

    def __init__(self):
        # rule_id → StabilityTracker
        self._trackers: Dict[str, StabilityTracker] = {}
        # rule_id → GenericAlarmRule
        self._rules: Dict[str, GenericAlarmRule] = {}

    def set_rules(self, rules: List[GenericAlarmRule]):
        """
        Load hoặc reload toàn bộ rule set.
        Các rule đã có tracker sẽ giữ nguyên trạng thái.
        """
        new_ids = {r.rule_id for r in rules}
        old_ids = set(self._rules.keys())

        # Xóa các rule đã bị remove
        for removed_id in (old_ids - new_ids):
            self._trackers.pop(removed_id, None)
            self._rules.pop(removed_id, None)
            logger.debug(f"[AlarmEvaluator] Removed rule: {removed_id}")

        # Thêm hoặc cập nhật rule
        for rule in rules:
            self._rules[rule.rule_id] = rule
            if rule.rule_id not in self._trackers:
                tracker = StabilityTracker(
                    stable_on_sec=rule.stable_on_sec,
                    stable_off_sec=rule.stable_off_sec,
                )
                self._trackers[rule.rule_id] = tracker
                logger.debug(f"[AlarmEvaluator] Added rule: {rule.rule_id}")
            else:
                # Cập nhật stable times nếu thay đổi (giữ state)
                t = self._trackers[rule.rule_id]
                t.stable_on_sec = rule.stable_on_sec
                t.stable_off_sec = rule.stable_off_sec

    def evaluate(
        self,
        signal_map: Dict[int, Signal],
        now: Optional[float] = None,
    ) -> List[AlarmEvent]:
        """
        Evaluate tất cả rules với signals hiện tại.

        Args:
            signal_map  — dict tag_id → Signal (snapshot)
            now         — timestamp để dùng cho debounce; default = time.time()

        Returns:
            List[AlarmEvent] — các events có state transition trong chu kỳ này
        """
        if now is None:
            now = time.time()

        events: List[AlarmEvent] = []

        for rule_id, rule in self._rules.items():
            tracker = self._trackers.get(rule_id)
            if tracker is None:
                continue

            old_state = tracker.current_state
            satisfied, trigger_tag_id, trigger_value = rule.evaluate_conditions(signal_map)
            new_state = tracker.update(satisfied, now=now)

            if new_state is not None:
                event = AlarmEvent(
                    rule_id=rule_id,
                    rule_label=rule.label,
                    from_state=old_state,
                    to_state=new_state,
                    trigger_value=trigger_value,
                    trigger_tag_id=trigger_tag_id,
                    timestamp=now,
                    extra={**rule.extra},
                )
                events.append(event)
                logger.debug(
                    f"[AlarmEvaluator] {rule_id}: "
                    f"{old_state.value} → {new_state.value} "
                    f"(tag={trigger_tag_id}, val={trigger_value})"
                )

        return events

    def get_state(self, rule_id: str) -> AlarmState:
        """Trả về trạng thái hiện tại của một rule."""
        tracker = self._trackers.get(rule_id)
        if tracker is None:
            return AlarmState.NORMAL
        return tracker.current_state

    def get_all_states(self) -> Dict[str, AlarmState]:
        """Trả về snapshot toàn bộ trạng thái."""
        return {rid: t.current_state for rid, t in self._trackers.items()}

    def reset_rule(self, rule_id: str):
        """Reset trạng thái của một rule cụ thể."""
        tracker = self._trackers.get(rule_id)
        if tracker:
            tracker.reset()

    def reset_all(self):
        """Reset toàn bộ trạng thái."""
        for tracker in self._trackers.values():
            tracker.reset()

    @property
    def rule_count(self) -> int:
        return len(self._rules)

    @property
    def active_alarm_count(self) -> int:
        """Số rule đang ở trạng thái ALARM."""
        return sum(
            1 for t in self._trackers.values()
            if t.current_state == AlarmState.ALARM
        )
