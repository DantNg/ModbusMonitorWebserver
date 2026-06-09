"""
test_teardown_alarm.py
======================
Xoá sạch toàn bộ dữ liệu test đã tạo bởi test_setup_alarm.py:
  - Device "[TEST] Alarm Device" và tất cả tags của nó
  - 2 Subdashboard "[TEST] Tag Monitor" và "[TEST] QTag Monitor"
  - Tất cả qtag cards + alarm conditions + alarm states trong các subdash đó
  - Alarm events / notifications liên quan đến test device

Chạy:
    .venv\\Scripts\\python.exe test_teardown_alarm.py
"""

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
_WEBAPP = os.path.join(_ROOT, "webapp")
if _WEBAPP not in sys.path:
    sys.path.insert(0, _WEBAPP)

import modbus_monitor.database.db as db
from modbus_monitor.database.db import init_engine
from sqlalchemy import select, delete, text

print("=" * 60)
print("  TEST TEARDOWN – Xoá dữ liệu test alarm")
print("=" * 60)

engine = init_engine()

# ── Tên nhận dạng dữ liệu test ────────────────────────────────────────────────
TEST_DEVICE_NAME   = "[TEST] Alarm Device"
TEST_DASH_PREFIX   = "[TEST]"

# ── Import các table objects từ db.py ─────────────────────────────────────────
from modbus_monitor.database.db import (
    devices, tags, tag_latest_values, tag_values, tag_logs,
    alarm_rules, alarm_events,
    dashboards, dashboard_tags,
    subdash_tag_groups, subdash_group_tags,
)

# ── Import các card/condition tables (có thể không export sẵn → dùng text SQL) ──
_CARD_TABLES = [
    ("subdash_quad_cards",         "quad",    "group_id"),
    ("subdash_qtag6_cards",        "qtag6",   "group_id"),
    ("subdash_qtag4_cards",        "qtag4",   "group_id"),
    ("subdash_qtag3_cards",        "qtag3",   "group_id"),
    ("subdash_qtag2_cards",        "qtag2",   "group_id"),
    ("subdash_qtag_single3_cards", "single3", "group_id"),
    ("subdash_qtag_pv_cards",      "pv_only", "group_id"),
    ("subdash_qtag_pv_dual_cards", "pv_dual", "group_id"),
]

_CARD_ALARM_CONDITION_TABLE = "card_alarm_conditions"
_CARD_ALARM_STATE_TABLE     = "card_alarm_states"
_QUAD_CONDITION_TABLE       = "quad_tag_conditions"
_QUAD_STATE_TABLE           = "quad_alarm_states"

# ==============================================================================
# 1. Tìm device test
# ==============================================================================
print(f"\n[1] Tìm device '{TEST_DEVICE_NAME}' ...")
with engine.connect() as con:
    row = con.execute(
        select(devices.c.id).where(devices.c.name == TEST_DEVICE_NAME)
    ).first()
    test_device_id = row[0] if row else None

if test_device_id is None:
    print(f"   ⚠️  Không tìm thấy device '{TEST_DEVICE_NAME}' — có thể đã xoá.")
else:
    print(f"   ✅ Tìm thấy device ID = {test_device_id}")

# ==============================================================================
# 2. Tìm các subdashboard test
# ==============================================================================
print(f"\n[2] Tìm các subdashboard '{TEST_DASH_PREFIX}*' ...")
with engine.connect() as con:
    rows = con.execute(
        select(dashboards.c.id, dashboards.c.name)
        .where(dashboards.c.name.like(f"{TEST_DASH_PREFIX}%"))
    ).all()
    test_dash_ids = [(r[0], r[1]) for r in rows]

if not test_dash_ids:
    print("   ⚠️  Không tìm thấy subdashboard test nào.")
else:
    for did, dname in test_dash_ids:
        print(f"   ✅ Subdashboard ID={did}: {dname}")

# ==============================================================================
# 3. Xoá alarm conditions + states cho tất cả cards trong các subdash test
# ==============================================================================
print("\n[3] Xoá alarm conditions + states của các card test ...")

def get_group_ids_for_dash(con, dash_id):
    rows = con.execute(
        select(subdash_tag_groups.c.id).where(subdash_tag_groups.c.dashboard_id == dash_id)
    ).all()
    return [r[0] for r in rows]

with engine.begin() as con:
    for dash_id, dash_name in test_dash_ids:
        group_ids = get_group_ids_for_dash(con, dash_id)
        if not group_ids:
            continue
        group_id_list = ",".join(str(g) for g in group_ids)

        for table_name, card_type, gid_col in _CARD_TABLES:
            # Lấy card IDs trong các group này
            card_rows = con.execute(
                text(f"SELECT id FROM `{table_name}` WHERE `{gid_col}` IN ({group_id_list})")
            ).all()
            card_ids = [r[0] for r in card_rows]
            if not card_ids:
                continue
            card_id_list = ",".join(str(c) for c in card_ids)

            if card_type == "quad":
                # Xoá quad states
                con.execute(text(
                    f"DELETE FROM `{_QUAD_STATE_TABLE}` WHERE quad_id IN ({card_id_list})"
                ))
                # Xoá quad conditions
                con.execute(text(
                    f"DELETE FROM `{_QUAD_CONDITION_TABLE}` WHERE quad_card_id IN ({card_id_list})"
                ))
            else:
                # Xoá card alarm states
                con.execute(text(
                    f"DELETE FROM `{_CARD_ALARM_STATE_TABLE}` "
                    f"WHERE card_type='{card_type}' AND card_id IN ({card_id_list})"
                ))
                # Xoá card alarm conditions
                con.execute(text(
                    f"DELETE FROM `{_CARD_ALARM_CONDITION_TABLE}` "
                    f"WHERE card_type='{card_type}' AND card_id IN ({card_id_list})"
                ))

            # Xoá cards
            con.execute(text(f"DELETE FROM `{table_name}` WHERE id IN ({card_id_list})"))
            print(f"   ✅ Xoá {len(card_ids)} card(s) [{card_type}] + conditions/states")

# ==============================================================================
# 4. Xoá các subdashboard (cascade xoá groups + tag mappings)
# ==============================================================================
print("\n[4] Xoá subdashboard test ...")
for dash_id, dash_name in test_dash_ids:
    n = db.delete_subdashboard_row(dash_id)
    print(f"   ✅ Đã xoá subdashboard ID={dash_id} '{dash_name}'")

# ==============================================================================
# 5. Xoá device + tags (delete_device_row cascade xoá alarm_rules, alarm_events,
#    tag_values, tag_latest_values)
# ==============================================================================
if test_device_id is not None:
    print(f"\n[5] Xoá device ID={test_device_id} + tất cả tags ...")
    db.delete_device_row(test_device_id)
    print(f"   ✅ Đã xoá device '{TEST_DEVICE_NAME}' và tất cả tags/alarm_rules/events")
else:
    print("\n[5] Bỏ qua xoá device (không tìm thấy).")

# ==============================================================================
# 6. Xoá notifications liên quan đến test (nếu còn sót)
# ==============================================================================
print("\n[6] Dọn notifications liên quan đến '[TEST]' ...")
try:
    with engine.begin() as con:
        result = con.execute(text(
            "DELETE FROM notifications WHERE message LIKE '%[TEST]%' OR title LIKE '%[TEST]%'"
        ))
        print(f"   ✅ Đã xoá {result.rowcount} notification(s) liên quan đến [TEST]")
except Exception as e:
    print(f"   ⚠️  Bỏ qua xoá notifications: {e}")

# ==============================================================================
# 7. Tóm tắt
# ==============================================================================
print("\n" + "=" * 60)
print("  TEARDOWN HOÀN TẤT!")
print("=" * 60)
print("""
✅ Đã xoá:
   - Device [TEST] Alarm Device + 10 tags
   - Alarm rules + alarm events của test tags
   - Subdashboard [TEST] Tag Monitor
   - Subdashboard [TEST] QTag Monitor + tất cả cards (8 loại)
   - Card alarm conditions + states cho tất cả test cards
   - Notifications liên quan đến [TEST]

⚡ Khởi động lại alarm_worker để cache được reset hoàn toàn.
""")
