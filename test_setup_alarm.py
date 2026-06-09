"""
test_setup_alarm.py
===================
Script tự động tạo dữ liệu test cho chức năng alarm:
  1. Tạo 1 device ModbusTCP (127.0.0.1)
  2. Tạo 10 tag
  3. Inject giá trị test vào tag_latest_values
  4. Tạo 2 subdashboard:
       - "Tag Monitor"  : hiển thị 10 tag dạng list
       - "QTag Monitor" : hiển thị đủ 8 loại qtag (quad, qtag6, qtag4, qtag3,
                          qtag2, single3, pv_only, pv_dual)
  5. Tạo alarm condition cho từng qtag card (stable = 3s để trigger nhanh)

Chạy script MỘT LẦN trước khi mở app:
    .venv\\Scripts\\python.exe test_setup_alarm.py

Sau khi chạy xong, mở app và quan sát alarm trigger trong ~5-10 giây.
"""

import sys

# ── DISABLED: Script này đã chạy xong, chặn chạy lại để tránh tạo dữ liệu trùng ──
print("⛔  test_setup_alarm.py đã bị vô hiệu hoá. Xoá dòng sys.exit() nếu muốn chạy lại.")
sys.exit(0)

import os
from datetime import datetime

# ── đưa webapp vào sys.path để import db ──────────────────────────────────────
_ROOT = os.path.dirname(os.path.abspath(__file__))
_WEBAPP = os.path.join(_ROOT, "webapp")
if _WEBAPP not in sys.path:
    sys.path.insert(0, _WEBAPP)

import modbus_monitor.database.db as db

# ==============================================================================
# 1. Cấu hình test data
# ==============================================================================

DEVICE_NAME = "[TEST] Alarm Device"
DEVICE_HOST = "127.0.0.1"
DEVICE_PORT = 502

# 10 tag: (name, address, unit, description, initial_value)
TEST_TAGS = [
    ("Temperature_1",  0, "°C",  "Nhiệt độ khu vực 1",   85.0),   # > 80 → HIGH alarm
    ("Temperature_2",  1, "°C",  "Nhiệt độ khu vực 2",   15.0),   # < 20 → LOW alarm
    ("Pressure_1",     2, "bar", "Áp suất đường 1",      80.0),   # SV threshold
    ("Pressure_2",     3, "bar", "Áp suất đường 2",      20.0),   # SV threshold
    ("Flow_1",         4, "L/m", "Lưu lượng 1",          75.5),   # > 70 → HIGH alarm
    ("Flow_2",         5, "L/m", "Lưu lượng 2",          45.0),
    ("Level_1",        6, "%",   "Mức nước bể 1",        55.0),   # > 50 → HIGH alarm
    ("Level_2",        7, "%",   "Mức nước bể 2",        35.0),
    ("Speed_1",        8, "RPM", "Tốc độ motor 1",       90.0),   # > 80 → HIGH alarm
    ("Speed_2",        9, "RPM", "Tốc độ motor 2",       50.0),   # > 40 → HIGH alarm
]

# Alarm conditions – stable 3s để trigger nhanh khi test
STABLE_ON  = 3
STABLE_OFF = 10

print("=" * 60)
print("  TEST SETUP – Alarm Feature")
print("=" * 60)

# ==============================================================================
# 2. Tạo device
# ==============================================================================
print("\n[1] Tạo device ModbusTCP 127.0.0.1 ...")
device_id = db.add_device_row({
    "name": DEVICE_NAME,
    "protocol": "ModbusTCP",
    "host": DEVICE_HOST,
    "port": DEVICE_PORT,
    "unit_id": 1,
    "timeout_ms": 2000,
    "read_interval_ms": 1000,
    "byte_order": "BigEndian",
    "default_function_code": 3,
    "description": "Device dùng để test chức năng alarm",
})
print(f"   ✅ Device ID = {device_id}")

# ==============================================================================
# 3. Tạo 10 tag + inject giá trị vào tag_latest_values
# ==============================================================================
print("\n[2] Tạo 10 tag ...")
tag_ids = []
for name, addr, unit, desc, _ in TEST_TAGS:
    tid = db.add_tag_row(device_id, {
        "name":        name,
        "address":     addr,
        "datatype":    "Float32",
        "unit":        unit,
        "scale":       1.0,
        "offset":      0.0,
        "grp":         "Group1",
        "description": desc,
    })
    tag_ids.append(tid)
    print(f"   ✅ Tag '{name}' → ID {tid}")

# Tag alias cho dễ đọc (0-indexed)
T = tag_ids  # T[0]..T[9]

print("\n[3] Inject giá trị test vào DB ...")
now = datetime.now()
bulk_rows = [(T[i], now, TEST_TAGS[i][4]) for i in range(10)]
db.insert_tag_values_bulk(bulk_rows)
for i, (name, _, _, _, val) in enumerate(TEST_TAGS):
    print(f"   ✅ {name} (ID {T[i]}) = {val}")

# ==============================================================================
# 4. Subdashboard 1 – Tag Monitor (danh sách thông thường)
# ==============================================================================
print("\n[4] Tạo Subdashboard 1: 'Tag Monitor' ...")
dash1_id = db.add_subdashboard_row({
    "name":        "[TEST] Tag Monitor",
    "description": "Hiển thị 10 tag dạng danh sách, không có qtag",
})
print(f"   ✅ Dashboard ID = {dash1_id}")

grp1_id = db.add_subdash_group({
    "dashboard_id": dash1_id,
    "name":         "All Tags",
    "order":        0,
})
for tid in tag_ids:
    db.add_tag_to_subdash_group(grp1_id, tid)
print(f"   ✅ Group 'All Tags' (ID {grp1_id}) – {len(tag_ids)} tags added")

# ==============================================================================
# 5. Subdashboard 2 – QTag Monitor (đủ 8 loại qtag)
# ==============================================================================
print("\n[5] Tạo Subdashboard 2: 'QTag Monitor' ...")
dash2_id = db.add_subdashboard_row({
    "name":        "[TEST] QTag Monitor",
    "description": "Hiển thị tất cả 8 loại qtag card với alarm condition",
})
print(f"   ✅ Dashboard ID = {dash2_id}")

def make_group(name, order):
    gid = db.add_subdash_group({
        "dashboard_id": dash2_id,
        "name":         name,
        "order":        order,
    })
    print(f"   ✅ Group '{name}' (ID {gid})")
    return gid

# ── 5a. Quad Card (tag1=PV left, tag2=PV right, tag3=SV left, tag4=SV right)
grp_quad   = make_group("Quad Tag Card",  0)
quad_id = db.add_quad_tag_card(
    group_id=grp_quad,
    tag1_id=T[0], tag2_id=T[1],    # PV left, PV right
    tag3_id=T[2], tag4_id=T[3],    # SV left,  SV right
    card_title="[TEST] Quad Card",
    left_title="Nhiệt độ 1",
    right_title="Nhiệt độ 2",
    sv_left_type="tag", sv_right_type="tag",
)
print(f"      quad_id = {quad_id}")

# ── 5b. Qtag6 Card (tag1=PV left, tag2=PV right, tag3=SV-H left, tag4=SV-H right,
#                    tag5=SV-L left, tag6=SV-L right)
grp_qtag6  = make_group("Qtag6 Card",  1)
qtag6_id = db.add_qtag6_card(
    group_id=grp_qtag6,
    tag1_id=T[0], tag2_id=T[1],
    tag3_id=T[2], tag4_id=T[3],
    tag5_id=T[4], tag6_id=T[5],
    card_title="[TEST] Qtag6",
    left_title="Nhiệt độ 1",
    right_title="Nhiệt độ 2",
)
print(f"      qtag6_id = {qtag6_id}")

# ── 5c. Qtag4 Card
grp_qtag4  = make_group("Qtag4 Card",  2)
qtag4_id = db.add_qtag4_card(
    group_id=grp_qtag4,
    tag1_id=T[0], tag2_id=T[1],
    tag3_id=T[2], tag4_id=T[3],
    card_title="[TEST] Qtag4",
    left_title="Nhiệt độ 1",
    right_title="Nhiệt độ 2",
)
print(f"      qtag4_id = {qtag4_id}")

# ── 5d. Qtag3 Card (PV, SV-HIGH, SV-LOW – single column)
grp_qtag3  = make_group("Qtag3 Card",  3)
qtag3_id = db.add_qtag3_card(
    group_id=grp_qtag3,
    tag1_id=T[4],   # PV
    tag2_id=T[2],   # SV HIGH
    tag3_id=T[3],   # SV LOW
    card_title="[TEST] Qtag3 – Flow 1",
    column_title="Flow 1",
)
print(f"      qtag3_id = {qtag3_id}")

# ── 5e. Qtag2 Card (PV + SV – single column)
grp_qtag2  = make_group("Qtag2 Card",  4)
qtag2_id = db.add_qtag2_card(
    group_id=grp_qtag2,
    tag1_id=T[6],   # PV
    tag2_id=T[7],   # SV
    card_title="[TEST] Qtag2 – Level 1",
    column_title="Level 1",
)
print(f"      qtag2_id = {qtag2_id}")

# ── 5f. Single3 Card (PV + SV HIGH fixed + SV LOW fixed)
grp_single3 = make_group("Single3 Card", 5)
single3_id = db.add_qtag_single3_card(
    group_id=grp_single3,
    pv_tag_id=T[8],          # Speed_1 = 90
    card_title="[TEST] Single3 – Speed 1",
    sv_high_type="fixed", sv_high_fixed=80.0,   # SV HIGH = 80
    sv_low_type="fixed",  sv_low_fixed=20.0,    # SV LOW  = 20
)
print(f"      single3_id = {single3_id}")

# ── 5g. PV Only Card
grp_pv_only = make_group("PV Only Card", 6)
pv_only_id = db.add_qtag_pv_card(
    group_id=grp_pv_only,
    pv_tag_id=T[9],          # Speed_2 = 50
    card_title="[TEST] PV Only – Speed 2",
    description="Chỉ hiển thị giá trị PV, alarm khi > 40",
)
print(f"      pv_only_id = {pv_only_id}")

# ── 5h. PV Dual Card (2 cột PV, không có SV)
grp_pv_dual = make_group("PV Dual Card", 7)
pv_dual_id = db.add_qtag_pv_dual_card(
    group_id=grp_pv_dual,
    left_tag_id=T[0],        # Temperature_1 = 85
    right_tag_id=T[1],       # Temperature_2 = 15
    card_title="[TEST] PV Dual – Temperature",
    left_title="Nhiệt độ 1",
    right_title="Nhiệt độ 2",
    left_description="Vùng 1 – Alarm > 80",
    right_description="Vùng 2 – Alarm < 20",
)
print(f"      pv_dual_id = {pv_dual_id}")

# ==============================================================================
# 6. Tạo alarm conditions cho từng qtag type
#    Dùng save_card_alarm_condition + save_quad_tag_condition
# ==============================================================================
print("\n[6] Tạo alarm conditions ...")

def cond(high_op=None, high_val=None, low_op=None, low_val=None,
         side="left", description=""):
    """Helper tạo dict condition cho một cột."""
    d = {}
    if high_op:
        d[f"{side}_high_operator"]    = high_op
        d[f"{side}_high_compare_type"] = "static"
        d[f"{side}_high_value"]        = high_val
        d[f"{side}_high_on_stable"]    = STABLE_ON
        d[f"{side}_high_off_stable"]   = STABLE_OFF
    if low_op:
        d[f"{side}_low_operator"]     = low_op
        d[f"{side}_low_compare_type"] = "static"
        d[f"{side}_low_value"]        = low_val
        d[f"{side}_low_on_stable"]    = STABLE_ON
        d[f"{side}_low_off_stable"]   = STABLE_OFF
    d[f"{side}_description"] = description
    d["enabled"] = True
    return d


# ── 6a. Quad Card alarm (dùng bảng quad_tag_conditions)
#    T[0]=85 → left HIGH > 80 sẽ trigger
#    T[1]=15 → right LOW < 20 sẽ trigger
quad_cond = {
    **cond(">=", 80.0, description="Nhiệt độ 1 quá cao",  side="left"),
    **cond(None, None, "<",  20.0, description="Nhiệt độ 2 quá thấp", side="right"),
    "enabled": True,
}
quad_cond_id = db.save_quad_condition(quad_id, quad_cond)
print(f"   ✅ Quad alarm condition → ID {quad_cond_id}")

# ── 6b. Qtag6 Card alarm
#    T[0]=85 left HIGH > 80 → trigger
#    T[1]=15 right LOW < 20 → trigger
qtag6_cond = {
    **cond(">=", 80.0, description="PV left quá cao",   side="left"),
    **cond(None, None, "<",  20.0, description="PV right quá thấp", side="right"),
    "enabled": True,
}
db.save_card_alarm_condition("qtag6", qtag6_id, qtag6_cond)
print(f"   ✅ Qtag6 alarm condition saved")

# ── 6c. Qtag4 Card alarm – left HIGH > 80
qtag4_cond = {
    **cond(">=", 80.0, description="PV left quá cao", side="left"),
    **cond(None, None, "<",  20.0, description="PV right quá thấp", side="right"),
    "enabled": True,
}
db.save_card_alarm_condition("qtag4", qtag4_id, qtag4_cond)
print(f"   ✅ Qtag4 alarm condition saved")

# ── 6d. Qtag3 Card alarm – left HIGH > 70
#    T[4]=75.5 → trigger HIGH
qtag3_cond = {
    **cond(">=", 70.0, description="Flow 1 quá cao", side="left"),
    "enabled": True,
}
db.save_card_alarm_condition("qtag3", qtag3_id, qtag3_cond)
print(f"   ✅ Qtag3 alarm condition saved")

# ── 6e. Qtag2 Card alarm – left HIGH > 50
#    T[6]=55 → trigger HIGH
qtag2_cond = {
    **cond(">=", 50.0, description="Level 1 quá cao", side="left"),
    "enabled": True,
}
db.save_card_alarm_condition("qtag2", qtag2_id, qtag2_cond)
print(f"   ✅ Qtag2 alarm condition saved")

# ── 6f. Single3 Card alarm – left HIGH > 80
#    T[8]=90 → trigger HIGH
single3_cond = {
    **cond(">=", 80.0, description="Speed 1 quá cao", side="left"),
    "enabled": True,
}
db.save_card_alarm_condition("single3", single3_id, single3_cond)
print(f"   ✅ Single3 alarm condition saved")

# ── 6g. PV Only Card alarm – left HIGH > 40
#    T[9]=50 → trigger HIGH
pv_only_cond = {
    **cond(">=", 40.0, description="Speed 2 quá cao", side="left"),
    "enabled": True,
}
db.save_card_alarm_condition("pv_only", pv_only_id, pv_only_cond)
print(f"   ✅ PV Only alarm condition saved")

# ── 6h. PV Dual Card alarm
#    T[0]=85 left HIGH > 80 → trigger
#    T[1]=15 right LOW < 20 → trigger
pv_dual_cond = {
    **cond(">=", 80.0, description="Nhiệt độ 1 quá cao",   side="left"),
    **cond(None, None, "<",  20.0, description="Nhiệt độ 2 quá thấp", side="right"),
    "enabled": True,
}
db.save_card_alarm_condition("pv_dual", pv_dual_id, pv_dual_cond)
print(f"   ✅ PV Dual alarm condition saved")

# ==============================================================================
# 7. In tóm tắt
# ==============================================================================
print("\n" + "=" * 60)
print("  SETUP HOÀN TẤT!")
print("=" * 60)
print(f"""
📌 Giá trị tag đã inject:
   Temperature_1 = 85.0 °C  (alarm HIGH >= 80  → SẼ TRIGGER)
   Temperature_2 = 15.0 °C  (alarm LOW  <  20  → SẼ TRIGGER)
   Pressure_1    = 80.0 bar (dùng làm SV threshold)
   Pressure_2    = 20.0 bar (dùng làm SV threshold)
   Flow_1        = 75.5 L/m (alarm HIGH >= 70  → SẼ TRIGGER)
   Flow_2        = 45.0 L/m
   Level_1       = 55.0 %   (alarm HIGH >= 50  → SẼ TRIGGER)
   Level_2       = 35.0 %
   Speed_1       = 90.0 RPM (alarm HIGH >= 80  → SẼ TRIGGER)
   Speed_2       = 50.0 RPM (alarm HIGH >= 40  → SẼ TRIGGER)

📊 Subdashboard đã tạo:
   [TEST] Tag Monitor  → ID {dash1_id}  (10 tags dạng danh sách)
   [TEST] QTag Monitor → ID {dash2_id}  (8 loại qtag card)

⚡ Alarm sẽ trigger sau {STABLE_ON}s khi alarm_worker đọc giá trị.
   Mở app → vào [TEST] QTag Monitor → quan sát card chuyển màu đỏ.

⚠️  Lưu ý: device 127.0.0.1:502 KHÔNG cần chạy thực.
    Alarm worker đọc từ tag_latest_values đã inject – không phụ thuộc Modbus.
""")
