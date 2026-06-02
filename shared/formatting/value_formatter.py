"""
Centralized Value Formatting Service
-------------------------------------
Chỉ file này mới được quyết định cách format giá trị để hiển thị.
Tất cả code render (dashboard, alarm, report, notification) phải gọi qua đây.

Formatting Rules:
    Rule A — Integer-scaled data:
        scale = 0.1   → 1 decimal  (vd: 12.3)
        scale = 0.01  → 2 decimals (vd: 12.34)
        scale = 1.0   → 0 decimals, hiển thị nguyên (vd: 42)

    Rule B — Float datatypes:
        float, float32, real, double, float64
        → Luôn 2 decimal places (vd: 12.30, 45.50)

    Rule C — Fixed value (do người dùng nhập):
        Giữ đúng số chữ số thập phân người dùng nhập, capped tại 2 dp.
        numeric_value  → dùng cho alarm calculation
        display_text   → dùng cho hiển thị UI/email/SMS
"""

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Optional, Union
import math


# ---------------------------------------------------------------------------
# Datatype classification
# ---------------------------------------------------------------------------

# Các kiểu Modbus đọc về dạng integer (Word, Short, DWord, ...)
_INTEGER_DATATYPES = frozenset({
    "word", "short", "dword", "dint", "bit",
    "signed", "unsigned", "long", "long_inverse",
    "hex", "binary",
    "int16", "uint16", "int32", "uint32",
})

# Các kiểu đọc về dạng float IEEE 754
_FLOAT_DATATYPES = frozenset({
    "float", "float32", "real",
    "double", "float64",
})


# ---------------------------------------------------------------------------
# Helper: tính decimal places từ scale
# ---------------------------------------------------------------------------

def _decimal_places_from_scale(scale: float) -> int:
    """
    Tính số chữ số thập phân dựa trên scale factor (Rule A).

        scale = 1.0   → 0
        scale = 0.1   → 1
        scale = 0.01  → 2
        scale = 0.001 → 3  (capped tại 3 trong thực tế)

    Trả về 0 nếu scale >= 1.0 hoặc không hợp lệ.
    """
    if not scale or scale >= 1.0 or scale <= 0:
        return 0
    try:
        s = Decimal(str(scale))
        places = 0
        while s < 1:
            s *= 10
            places += 1
        return places
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Domain classes
# ---------------------------------------------------------------------------

class TagFormatMetadata:
    """
    Thông tin tag cần thiết để quyết định cách format giá trị.
    Được tạo từ row trong bảng 'tags'.
    """
    __slots__ = ("tag_id", "scale", "offset", "unit", "datatype")

    def __init__(
        self,
        tag_id: int,
        scale: float = 1.0,
        offset: float = 0.0,
        unit: str = "",
        datatype: str = "unsigned",
    ):
        self.tag_id = tag_id
        self.scale = float(scale) if scale else 1.0
        self.offset = float(offset) if offset else 0.0
        self.unit = unit or ""
        self.datatype = (datatype or "unsigned").strip().lower()

    @classmethod
    def from_db_row(cls, row: dict) -> "TagFormatMetadata":
        """Tạo TagFormatMetadata từ dict row của bảng tags."""
        return cls(
            tag_id=int(row["id"]),
            scale=float(row.get("scale") or 1.0),
            offset=float(row.get("offset") or 0.0),
            unit=row.get("unit") or "",
            datatype=row.get("datatype") or "unsigned",
        )

    def __repr__(self):
        return (
            f"TagFormatMetadata(id={self.tag_id}, scale={self.scale}, "
            f"unit='{self.unit}', datatype='{self.datatype}')"
        )


class DisplayValue:
    """
    Kết quả sau khi format:
        numeric_value  → dùng cho alarm calculation, DB storage
        display_text   → dùng cho UI render, email, SMS, report
    """
    __slots__ = ("numeric_value", "display_text", "unit", "decimal_places", "is_fixed")

    def __init__(
        self,
        numeric_value: Optional[float],
        display_text: str,
        unit: str = "",
        decimal_places: int = 0,
        is_fixed: bool = False,
    ):
        self.numeric_value = numeric_value
        self.display_text = display_text
        self.unit = unit or ""
        self.decimal_places = decimal_places
        self.is_fixed = is_fixed

    def __str__(self):
        if self.unit:
            return f"{self.display_text} {self.unit}"
        return self.display_text

    def __repr__(self):
        return (
            f"DisplayValue(numeric={self.numeric_value}, "
            f"text='{self.display_text}', unit='{self.unit}', "
            f"dp={self.decimal_places}, fixed={self.is_fixed})"
        )


# ---------------------------------------------------------------------------
# Core Formatter
# ---------------------------------------------------------------------------

class ValueFormatter:
    """
    Formatting Service tập trung.

    Usage (singleton):
        formatter = ValueFormatter.get_instance()
        dv = formatter.format_tag_value(105.3, meta)
        print(dv.display_text)  # "105.3" nếu scale=0.1

    Usage (module-level shortcuts):
        from shared.formatting import format_tag_value, TagFormatMetadata
        dv = format_tag_value(105.3, TagFormatMetadata(tag_id=1, scale=0.1))
    """

    _instance: Optional["ValueFormatter"] = None

    @classmethod
    def get_instance(cls) -> "ValueFormatter":
        """Trả về singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def format_tag_value(
        self,
        value: Optional[Union[int, float]],
        meta: TagFormatMetadata,
    ) -> DisplayValue:
        """
        Format giá trị live của một tag theo Rule A hoặc B.

        Rule A — Integer datatypes với scale < 1:
            scale=0.1  → 1 dp, scale=0.01 → 2 dp
        Rule B — Float datatypes:
            Luôn 2 dp bất kể scale
        Integer datatypes với scale=1.0:
            Hiển thị dạng số nguyên (không có dấu thập phân)
        """
        if value is None:
            return DisplayValue(None, "--", meta.unit)
        try:
            fval = float(value)
        except (ValueError, TypeError):
            return DisplayValue(None, "--", meta.unit)
        if math.isnan(fval) or math.isinf(fval):
            return DisplayValue(None, "--", meta.unit)

        dtype = meta.datatype

        # Rule B: Float datatypes → luôn 2 dp
        if dtype in _FLOAT_DATATYPES:
            return self._format_float(fval, meta.unit, decimal_places=2)

        # Rule A: Integer datatypes
        if dtype in _INTEGER_DATATYPES:
            dp = _decimal_places_from_scale(meta.scale)
            if dp == 0:
                # Integer display — loại bỏ .0 cuối
                try:
                    int_val = int(round(fval))
                    return DisplayValue(int_val, str(int_val), meta.unit, 0)
                except (ValueError, OverflowError):
                    return DisplayValue(None, "--", meta.unit)
            return self._format_float(fval, meta.unit, decimal_places=dp)

        # Fallback cho các kiểu không xác định: dùng 2 dp
        return self._format_float(fval, meta.unit, decimal_places=2)

    def format_fixed_value(
        self,
        raw_input: Optional[str],
    ) -> DisplayValue:
        """
        Rule C: Giữ đúng số chữ số thập phân người dùng nhập, capped tại 2 dp.

        Examples:
            "12.00"   → DisplayValue(12.0,   "12.00",  dp=2)
            "13.01"   → DisplayValue(13.01,  "13.01",  dp=2)
            "150.000" → DisplayValue(150.0,  "150.00", dp=2)  # capped at 2
            "500"     → DisplayValue(500,    "500",    dp=0)
            "500.1"   → DisplayValue(500.1,  "500.1",  dp=1)

        QUAN TRỌNG:
            numeric_value → dùng cho alarm calculation (float/int chính xác)
            display_text  → dùng cho UI, email, SMS (giữ đúng format người nhập)
        """
        if raw_input is None:
            return DisplayValue(None, "--", is_fixed=True)

        text = str(raw_input).strip()
        if not text:
            return DisplayValue(None, "--", is_fixed=True)

        try:
            number = Decimal(text)
        except (InvalidOperation, ValueError):
            return DisplayValue(None, "--", is_fixed=True)

        # Xác định decimal places từ chuỗi gốc, capped tại 2
        dp = 0
        if "." in text:
            fraction = text.split(".", 1)[1]
            dp = min(2, len(fraction))

        # Quantize về đúng dp
        if dp == 0:
            normalized = number.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            numeric = int(normalized)
            display_text = str(numeric)
        else:
            pattern = Decimal("0." + "0" * (dp - 1) + "1")
            normalized = number.quantize(pattern, rounding=ROUND_HALF_UP)
            numeric = float(normalized)
            display_text = f"{float(normalized):.{dp}f}"

        return DisplayValue(
            numeric_value=numeric,
            display_text=display_text,
            decimal_places=dp,
            is_fixed=True,
        )

    def format_alarm_value(
        self,
        value: Optional[float],
        meta: Optional[TagFormatMetadata] = None,
    ) -> str:
        """
        Format giá trị tại thời điểm alarm cho log/notification.
        Nếu có tag metadata, dùng cùng rule với format_tag_value.
        Nếu không, áp dụng heuristic: int nếu nguyên, 2 dp nếu có phần lẻ.
        """
        if value is None:
            return "--"
        if meta:
            return self.format_tag_value(value, meta).display_text
        # Fallback heuristic
        try:
            fval = float(value)
            if math.isnan(fval):
                return "--"
            if fval == int(fval):
                return str(int(fval))
            return f"{fval:.2f}"
        except (ValueError, TypeError):
            return "--"

    def format_alarm_threshold(
        self,
        threshold: Optional[float],
        meta: Optional[TagFormatMetadata] = None,
    ) -> str:
        """
        Format ngưỡng alarm để hiển thị trong email, SMS, alarm log.
        Dùng cùng rule với format_tag_value nếu có tag metadata.
        """
        if threshold is None:
            return "--"
        return self.format_alarm_value(threshold, meta)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _format_float(
        value: float,
        unit: str,
        decimal_places: int,
    ) -> DisplayValue:
        """Format một float với số dp cố định."""
        try:
            if math.isnan(value) or math.isinf(value):
                return DisplayValue(None, "--", unit)
            display_text = f"{value:.{decimal_places}f}"
            return DisplayValue(value, display_text, unit, decimal_places)
        except (ValueError, TypeError, OverflowError):
            return DisplayValue(None, "--", unit)


# ---------------------------------------------------------------------------
# Module-level singleton instance (khởi tạo lazy)
# ---------------------------------------------------------------------------

_formatter = ValueFormatter()


def format_tag_value(
    value: Optional[Union[int, float]],
    meta: TagFormatMetadata,
) -> DisplayValue:
    """Shortcut: format giá trị live của một tag."""
    return _formatter.format_tag_value(value, meta)


def format_fixed_value(raw_input: Optional[str]) -> DisplayValue:
    """Shortcut: format fixed value (Rule C)."""
    return _formatter.format_fixed_value(raw_input)


def format_alarm_value(
    value: Optional[float],
    meta: Optional[TagFormatMetadata] = None,
) -> str:
    """Shortcut: format giá trị tại thời điểm alarm."""
    return _formatter.format_alarm_value(value, meta)


def format_alarm_threshold(
    threshold: Optional[float],
    meta: Optional[TagFormatMetadata] = None,
) -> str:
    """Shortcut: format ngưỡng alarm."""
    return _formatter.format_alarm_threshold(threshold, meta)
