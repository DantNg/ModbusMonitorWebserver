"""
Card Alarm Translator
----------------------
Pure transformation module: map DB rows từ `alarm_rules` hoặc
`card_alarm_conditions` → GenericAlarmRule để feed vào AlarmEvaluator.

Không có DB queries, không có Flask context, không có imports từ webapp.
Caller chịu trách nhiệm fetch dữ liệu từ DB rồi truyền vào đây.

Sơ đồ mapping:
    DB row (alarm_rules):
        id, name, tag_id, operator, threshold_value,
        severity, on_stable_sec, off_stable_sec, enabled

    DB row (card_alarm_conditions):
        alarm_condition_id, card_id, card_type, tag_id,
        condition_type, operator, threshold_value, compare_tag_id,
        severity, on_stable_sec, off_stable_sec

    → GenericAlarmRule với conditions list
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Any

from .alarm_engine_v2 import (
    Condition,
    StaticThresholdCondition,
    TagCompareCondition,
    GenericAlarmRule,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Map từ bảng alarm_rules (shared/alarm_engine.py hiện tại)
# ---------------------------------------------------------------------------

def translate_alarm_rule_row(row: Dict[str, Any]) -> Optional[GenericAlarmRule]:
    """
    Convert một row từ bảng `alarm_rules` → GenericAlarmRule.

    Row schema:
        id, name, tag_id, operator, threshold_value,
        severity, on_stable_sec, off_stable_sec, enabled

    Trả về None nếu row không hợp lệ hoặc enabled=False.
    """
    try:
        if not row.get("enabled", True):
            return None

        rule_id = f"alarm_rule_{row['id']}"
        label = row.get("name") or f"Rule #{row['id']}"
        tag_id = int(row["tag_id"])
        operator = row.get("operator", ">")
        threshold = float(row.get("threshold_value", 0.0))
        severity = row.get("severity", "medium") or "medium"
        on_sec = float(row.get("on_stable_sec") or 0.0)
        off_sec = float(row.get("off_stable_sec") or 0.0)

        condition = StaticThresholdCondition(
            tag_id=tag_id,
            operator=operator,
            threshold=threshold,
        )

        return GenericAlarmRule(
            rule_id=rule_id,
            label=label,
            conditions=[condition],
            severity=severity,
            stable_on_sec=on_sec,
            stable_off_sec=off_sec,
            extra={"source": "alarm_rules", "db_id": row["id"]},
        )

    except (KeyError, ValueError, TypeError) as exc:
        logger.warning(f"[translate_alarm_rule_row] Bỏ qua row không hợp lệ: {exc} — {row}")
        return None


def translate_alarm_rules_table(rows: List[Dict[str, Any]]) -> List[GenericAlarmRule]:
    """
    Convert toàn bộ rows từ bảng `alarm_rules` → list GenericAlarmRule.
    Tự động bỏ qua các row không hợp lệ hoặc disabled.

    Args:
        rows — list dict row (từ DB hoặc mock)

    Returns:
        List[GenericAlarmRule]
    """
    result = []
    for row in rows:
        rule = translate_alarm_rule_row(row)
        if rule is not None:
            result.append(rule)
    logger.debug(f"[translate_alarm_rules_table] {len(result)}/{len(rows)} rules hợp lệ.")
    return result


# ---------------------------------------------------------------------------
# Map từ bảng card_alarm_conditions
# ---------------------------------------------------------------------------

def translate_card_condition_row(
    row: Dict[str, Any],
    card_id: int,
    card_type: str,
) -> Optional[GenericAlarmRule]:
    """
    Convert một row từ bảng `card_alarm_conditions` → GenericAlarmRule.

    Row schema:
        alarm_condition_id, tag_id, condition_type ("static" | "tag_compare"),
        operator, threshold_value, compare_tag_id,
        severity, on_stable_sec, off_stable_sec

    Args:
        row       — dict row từ bảng card_alarm_conditions
        card_id   — ID card để tạo rule_id unique
        card_type — loại card để đính kèm vào extra (không ảnh hưởng logic)

    Trả về None nếu row không hợp lệ.
    """
    try:
        condition_id = int(row["alarm_condition_id"])
        tag_id = int(row["tag_id"])
        condition_type = row.get("condition_type", "static").strip().lower()
        operator = row.get("operator", ">")
        severity = row.get("severity", "medium") or "medium"
        on_sec = float(row.get("on_stable_sec") or 0.0)
        off_sec = float(row.get("off_stable_sec") or 0.0)

        rule_id = f"card_{card_type}_{card_id}_cond_{condition_id}"
        label = row.get("label") or f"{card_type.upper()} Card #{card_id} — Condition #{condition_id}"

        # Chọn loại condition
        if condition_type == "tag_compare":
            compare_tag_id = row.get("compare_tag_id")
            if compare_tag_id is None:
                logger.warning(
                    f"[translate_card_condition_row] tag_compare thiếu compare_tag_id: {row}"
                )
                return None
            condition: Condition = TagCompareCondition(
                tag_a_id=tag_id,
                operator=operator,
                tag_b_id=int(compare_tag_id),
            )
        else:
            # "static" (default)
            threshold = float(row.get("threshold_value") or 0.0)
            condition = StaticThresholdCondition(
                tag_id=tag_id,
                operator=operator,
                threshold=threshold,
            )

        return GenericAlarmRule(
            rule_id=rule_id,
            label=label,
            conditions=[condition],
            severity=severity,
            stable_on_sec=on_sec,
            stable_off_sec=off_sec,
            extra={
                "source": "card_alarm_conditions",
                "card_id": card_id,
                "card_type": card_type,
                "condition_id": condition_id,
            },
        )

    except (KeyError, ValueError, TypeError) as exc:
        logger.warning(
            f"[translate_card_condition_row] Bỏ qua row không hợp lệ: {exc} — {row}"
        )
        return None


def translate_card_conditions_batch(
    rows: List[Dict[str, Any]],
    card_id: int,
    card_type: str,
) -> List[GenericAlarmRule]:
    """
    Convert toàn bộ conditions của một card → list GenericAlarmRule.
    Tự động bỏ qua các row không hợp lệ.

    Args:
        rows      — list dict row từ card_alarm_conditions
        card_id   — ID card
        card_type — loại card

    Returns:
        List[GenericAlarmRule]
    """
    result = []
    for row in rows:
        rule = translate_card_condition_row(row, card_id, card_type)
        if rule is not None:
            result.append(rule)
    logger.debug(
        f"[translate_card_conditions_batch] card={card_type}/{card_id}: "
        f"{len(result)}/{len(rows)} conditions hợp lệ."
    )
    return result
