"""
Alarm Engine Package
--------------------
Generic alarm evaluation engine — không phụ thuộc vào card types.

Usage:
    from shared.engine import AlarmEvaluator, Signal, GenericAlarmRule
    from shared.engine import StaticThresholdCondition, TagCompareCondition
    from shared.engine import translate_alarm_rules_table
"""

from .alarm_engine_v2 import (
    Signal,
    AlarmState,
    AlarmEvent,
    StaticThresholdCondition,
    TagCompareCondition,
    GenericAlarmRule,
    AlarmEvaluator,
)

from .card_alarm_translator import (
    translate_card_condition_row,
    translate_alarm_rules_table,
)

__all__ = [
    "Signal",
    "AlarmState",
    "AlarmEvent",
    "StaticThresholdCondition",
    "TagCompareCondition",
    "GenericAlarmRule",
    "AlarmEvaluator",
    "translate_card_condition_row",
    "translate_alarm_rules_table",
]
