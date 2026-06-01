"""Shared formatting helpers used across web and worker layers.

This module centralizes display rules while preserving the current output
contracts expected by templates, logs, and legacy callers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


KNOWN_DATATYPES = {
    'float', 'float32', 'real', 'float_inverse', 'floatinverse', 'float-inverse',
    'double', 'float64', 'double_inverse', 'doubleinverse', 'double-inverse',
    'hex', 'binary', 'bit', 'bool', 'boolean', 'raw',
    'signed', 'unsigned', 'word', 'short', 'dword', 'dint', 'long',
    'long_inverse', 'longinverse', 'long-inverse', 'int16', 'int32',
    'uint16', 'uint32', 'ushort', 'udint', 'int64'
}


@dataclass(frozen=True)
class FormatArgs:
    """Normalized arguments for legacy template filter signatures."""

    scale: Optional[float]
    offset: object
    datatype: Optional[str]


def parse_legacy_format_args(arg1=None, arg2=None, arg3=None) -> FormatArgs:
    """Preserve backward-compatible filter calling conventions.

    Supported signatures:
    - format_value(value, datatype)
    - format_value(value, scale, datatype)
    - format_value(value, scale, offset)
    - format_value(value, scale, offset, datatype)
    """

    datatype = None
    scale = None
    offset = 0

    if isinstance(arg1, str) and arg1.lower() in KNOWN_DATATYPES:
        datatype = arg1
        scale = arg2
        offset = arg3
    else:
        scale = arg1
        if isinstance(arg2, str) and arg2.lower() in KNOWN_DATATYPES:
            datatype = arg2
            offset = arg3
        else:
            offset = arg2
            datatype = arg3

    return FormatArgs(scale=scale, offset=offset, datatype=datatype)


def scale_to_decimal_places(scale_value) -> Optional[int]:
    """Infer decimal precision from scale text without trailing zeros."""

    try:
        scale_float = abs(float(scale_value))
        if scale_float == 0.0:
            return 0
        scale_text = f"{scale_float:.12f}".rstrip('0').rstrip('.')
        if '.' not in scale_text:
            return 0
        return min(6, len(scale_text.split('.', 1)[1]))
    except Exception:
        return None


def display_decimals_for_scale(scale_value) -> Optional[int]:
    """Return the display precision policy currently used by the UI."""

    try:
        scale_float = abs(float(scale_value))
    except Exception:
        return None

    if abs(scale_float - 1.0) < 1e-9:
        return None
    if abs(scale_float - 0.1) < 1e-9:
        return 1
    if scale_float >= 0.2:
        return 2
    return scale_to_decimal_places(scale_float)


def format_display_value(value, scale=None, offset=0, datatype=None, none_placeholder='—') -> str:
    """Format values for web/template display while preserving current behavior."""

    if value is None or value == '':
        return none_placeholder

    try:
        num_value = float(value)
    except (ValueError, TypeError):
        return str(value)

    if not (num_value == num_value) or abs(num_value) == float('inf'):
        return none_placeholder

    if num_value == 0.0:
        num_value = 0.0

    if datatype:
        datatype_lower = datatype.lower()
        if datatype_lower == 'hex':
            int_val = int(abs(num_value))
            return f"0x{int_val:X}"
        if datatype_lower in ('binary', 'bit', 'bool', 'boolean'):
            int_val = int(abs(num_value))
            return f"0b{int_val:b}"
        if datatype_lower == 'raw':
            return str(num_value)

    try:
        scale_float = abs(float(scale)) if scale is not None else None
    except Exception:
        scale_float = None

    if scale_float is not None and abs(scale_float - 1.0) < 1e-9:
        return f"{int(num_value)}" if num_value.is_integer() else f"{round(num_value, 2):g}"

    decimals = display_decimals_for_scale(scale)
    if decimals is not None:
        return f"{num_value:.{decimals}f}"

    if datatype:
        datatype_lower = datatype.lower()
        if datatype_lower in ('float', 'float32', 'real', 'float_inverse', 'floatinverse', 'float-inverse'):
            return f"{num_value:.2f}"
        if datatype_lower in ('double', 'float64', 'double_inverse', 'doubleinverse', 'double-inverse'):
            return f"{num_value:.4f}"

    if num_value.is_integer():
        return f"{int(num_value)}"
    return f"{round(num_value, 2):g}"


def format_fixed_value(value, decimal_places=None, none_placeholder='—') -> str:
    """Format fixed SV values preserving the current UI rendering rules."""

    if value is None or value == '':
        return none_placeholder

    try:
        num_value = float(value)
    except (ValueError, TypeError):
        return str(value)

    if abs(num_value) < 1e-12:
        num_value = 0.0

    try:
        dp = int(decimal_places) if decimal_places is not None else None
    except (ValueError, TypeError):
        dp = None

    if dp is None:
        return str(int(num_value)) if float(num_value).is_integer() else str(float(f"{num_value:.2f}"))

    dp = max(0, min(2, dp))
    if dp == 0:
        return f"{int(round(num_value))}"

    rendered = f"{num_value:.{dp}f}"
    if '.' in rendered:
        rendered = rendered.rstrip('0').rstrip('.')
    if '.' not in rendered:
        rendered = f"{rendered}.0"
    return rendered


def format_logger_value(value, scale=1.0, offset=0.0) -> str:
    """Format logger preview text preserving the worker's current behavior.

    Important: logger preview currently applies scale/offset directly before
    formatting. This helper keeps that exact legacy behavior to avoid changing
    production logs in this refactor step.
    """

    try:
        raw_value = float(value)
        scale_value = float(scale if scale is not None else 1.0)
        offset_value = float(offset if offset is not None else 0.0)
        display_value = (raw_value * scale_value) + offset_value

        if abs(scale_value - 0.1) < 1e-9:
            return f"{display_value:.1f}"
        if abs(scale_value - 0.2) < 1e-9:
            return f"{display_value:.2f}"

        if float(display_value).is_integer():
            return f"{int(display_value)}"
        return f"{display_value:.2f}"
    except Exception:
        return str(value)