"""
Script kiểm tra các tag có giá trị NULL hoặc timestamp NULL trong tag_latest_values
"""
import sys
import os

# Add webapp to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'webapp'))

from webapp.modbus_monitor.database.db import init_engine
from sqlalchemy import select, text

def check_null_tags():
    """Kiểm tra tags với giá trị NULL hoặc timestamp NULL"""
    
    with init_engine().connect() as con:
        # Tìm tags có value hoặc ts = NULL
        result = con.execute(text("""
            SELECT 
                tlv.tag_id,
                t.name as tag_name,
                t.device_id,
                d.name as device_name,
                tlv.value,
                tlv.ts,
                tlv.updated_at
            FROM tag_latest_values tlv
            JOIN tags t ON tlv.tag_id = t.id
            LEFT JOIN devices d ON t.device_id = d.id
            WHERE tlv.value IS NULL OR tlv.ts IS NULL
            ORDER BY t.device_id, t.id
        """)).fetchall()
        
        if result:
            print(f"\n⚠️  Tìm thấy {len(result)} tags có giá trị NULL:\n")
            print(f"{'Tag ID':<10} {'Tag Name':<30} {'Device':<20} {'Value':<10} {'Timestamp':<20} {'Updated':<20}")
            print("="*120)
            
            for row in result:
                tag_id, tag_name, device_id, device_name, value, ts, updated = row
                value_str = str(value) if value is not None else "NULL"
                ts_str = str(ts) if ts is not None else "NULL"
                updated_str = str(updated) if updated is not None else "NULL"
                print(f"{tag_id:<10} {tag_name:<30} {device_name or 'N/A':<20} {value_str:<10} {ts_str:<20} {updated_str:<20}")
        else:
            print("\n✅ Không có tag nào bị NULL trong tag_latest_values")
        
        # Kiểm tra tags không có record trong tag_latest_values
        result2 = con.execute(text("""
            SELECT 
                t.id as tag_id,
                t.name as tag_name,
                t.device_id,
                d.name as device_name
            FROM tags t
            LEFT JOIN devices d ON t.device_id = d.id
            LEFT JOIN tag_latest_values tlv ON t.id = tlv.tag_id
            WHERE tlv.tag_id IS NULL
            ORDER BY t.device_id, t.id
        """)).fetchall()
        
        if result2:
            print(f"\n⚠️  Tìm thấy {len(result2)} tags CHƯA CÓ dữ liệu trong tag_latest_values:\n")
            print(f"{'Tag ID':<10} {'Tag Name':<30} {'Device':<20}")
            print("="*60)
            
            for row in result2:
                tag_id, tag_name, device_id, device_name = row
                print(f"{tag_id:<10} {tag_name:<30} {device_name or 'N/A':<20}")
        else:
            print("\n✅ Tất cả tags đều có dữ liệu trong tag_latest_values")
        
        # Thống kê tổng quan
        stats = con.execute(text("""
            SELECT 
                COUNT(*) as total_tags,
                COUNT(tlv.tag_id) as tags_with_data,
                COUNT(*) - COUNT(tlv.tag_id) as tags_without_data,
                SUM(CASE WHEN tlv.value IS NULL THEN 1 ELSE 0 END) as null_values,
                SUM(CASE WHEN tlv.ts IS NULL THEN 1 ELSE 0 END) as null_timestamps
            FROM tags t
            LEFT JOIN tag_latest_values tlv ON t.id = tlv.tag_id
        """)).first()
        
        print(f"\n📊 THỐNG KÊ:")
        print(f"  - Tổng số tags: {stats[0]}")
        print(f"  - Tags có dữ liệu: {stats[1]}")
        print(f"  - Tags chưa có dữ liệu: {stats[2]}")
        print(f"  - Tags có value NULL: {stats[3]}")
        print(f"  - Tags có timestamp NULL: {stats[4]}")

if __name__ == "__main__":
    print("🔍 Kiểm tra tags có giá trị NULL trong tag_latest_values...\n")
    check_null_tags()
