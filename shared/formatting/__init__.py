"""
Formatting Service Package
--------------------------
Cung cấp API tập trung để format giá trị hiển thị.

Usage:
    from shared.formatting import format_tag_value, TagFormatMetadata
    from shared.formatting import format_fixed_value
    from shared.formatting import format_alarm_value, format_alarm_threshold
"""

from .value_formatter import (
    ValueFormatter,
    TagFormatMetadata,
    DisplayValue,
    format_tag_value,
    format_fixed_value,
    format_alarm_threshold,
    format_alarm_value,
)

__all__ = [
    "ValueFormatter",
    "TagFormatMetadata",
    "DisplayValue",
    "format_tag_value",
    "format_fixed_value",
    "format_alarm_threshold",
    "format_alarm_value",
]
