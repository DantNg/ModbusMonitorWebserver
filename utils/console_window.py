# -*- coding: utf-8 -*-
"""
console_window.py — tiện ích xử lý cửa sổ console của chính process.

Dùng cho các bản build có console (--console). Khi worker/exe được khởi chạy
độc lập (vd: webserver reload worker bằng `start "" worker.exe`), nó sẽ mở một
cửa sổ console mới. Gọi `minimize_console()` ngay khi khởi động để tự thu nhỏ
cửa sổ đó xuống taskbar thay vì bật lên trước mặt.

An toàn trong mọi trường hợp:
- Không phải Windows  -> no-op
- Bản build --noconsole / không có console -> no-op (GetConsoleWindow trả 0)
- Lỗi WinAPI bất kỳ   -> nuốt lỗi, không bao giờ làm sập worker
"""

import os

# ShowWindow nCmdShow constants
_SW_MINIMIZE = 6   # thu nhỏ, vẫn nằm trên taskbar
_SW_HIDE = 0       # ẩn hẳn (không hiện cả trên taskbar)


def _show_console(n_cmd_show: int) -> bool:
    if os.name != "nt":
        return False
    try:
        import ctypes
        hwnd = ctypes.windll.kernel32.GetConsoleWindow()
        if not hwnd:
            # Không có console (bản --noconsole) -> không có gì để làm
            return False
        ctypes.windll.user32.ShowWindow(hwnd, n_cmd_show)
        return True
    except Exception:
        # Việc chỉnh cửa sổ chỉ mang tính thẩm mỹ, tuyệt đối không để nó làm sập app
        return False


def minimize_console() -> bool:
    """Thu nhỏ cửa sổ console của process hiện tại xuống taskbar.

    Trả về True nếu đã thu nhỏ, False nếu không có console / không phải Windows.
    """
    return _show_console(_SW_MINIMIZE)


def hide_console() -> bool:
    """Ẩn hẳn cửa sổ console (không hiện cả trên taskbar)."""
    return _show_console(_SW_HIDE)
