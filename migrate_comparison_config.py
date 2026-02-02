"""
Migration script: Thêm cột logger_id vào bảng user_report_comparison_configs
Chạy script này để cập nhật database schema

Yêu cầu: Phải chạy trong virtual environment (.venv)
"""

import sys
import os
import json

# Đọc cấu hình database từ config/SMTP_config.json
def read_db_config():
    """Đọc cấu hình database từ config/SMTP_config.json"""
    config_file = os.path.join(os.path.dirname(__file__), 'config', 'SMTP_config.json')
    
    if not os.path.exists(config_file):
        print(f"❌ Không tìm thấy file config tại: {config_file}")
        sys.exit(1)
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Parse MYSQL_URI để lấy thông tin kết nối
    # Format: mysql+pymysql://user:password@host:port/database
    uri = config.get("MYSQL_URI", "mysql+pymysql://root:123456@localhost:3306/modbus_monitor_db")
    
    # Parse URI
    import re
    pattern = r'mysql\+pymysql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
    match = re.match(pattern, uri)
    
    if not match:
        print(f"❌ Không thể parse MYSQL_URI: {uri}")
        sys.exit(1)
    
    user, password, host, port, database = match.groups()
    
    db_config = {
        'host': host,
        'port': int(port),
        'user': user,
        'password': password,
        'database': database
    }
    
    return db_config

def get_db_connection():
    """Tạo connection đến database"""
    try:
        import pymysql
    except ImportError:
        print("❌ Không tìm thấy module pymysql. Đang cài đặt...")
        os.system("pip install pymysql")
        import pymysql
    
    db_config = read_db_config()
    
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
        return conn
    except Exception as e:
        print(f"❌ Không thể kết nối database: {e}")
        print(f"Database config: host={db_config['host']}, port={db_config['port']}, user={db_config['user']}, database={db_config['database']}")
        sys.exit(1)

def migrate_comparison_config_table():
    """
    Thêm cột logger_id vào bảng user_report_comparison_configs
    và cập nhật unique constraint
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("Bắt đầu migration bảng user_report_comparison_configs...")
        
        # Kiểm tra xem cột logger_id đã tồn tại chưa
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'user_report_comparison_configs' 
            AND COLUMN_NAME = 'logger_id'
        """)
        column_exists = cursor.fetchone()[0] > 0
        
        if column_exists:
            print("✓ Cột logger_id đã tồn tại, không cần thêm")
        else:
            print("Đang thêm cột logger_id...")
            # Thêm cột logger_id với giá trị mặc định là 'all'
            cursor.execute("""
                ALTER TABLE user_report_comparison_configs 
                ADD COLUMN logger_id VARCHAR(50) NOT NULL DEFAULT 'all'
                AFTER user_id
            """)
            conn.commit()
            print("✓ Đã thêm cột logger_id")
        
        # Xóa unique constraint cũ nếu tồn tại
        print("Đang kiểm tra unique constraint cũ...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.TABLE_CONSTRAINTS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'user_report_comparison_configs' 
            AND CONSTRAINT_NAME = 'uq_user_comparison_config'
        """)
        old_constraint_exists = cursor.fetchone()[0] > 0
        
        if old_constraint_exists:
            print("Đang xóa unique constraint cũ...")
            cursor.execute("""
                ALTER TABLE user_report_comparison_configs 
                DROP INDEX uq_user_comparison_config
            """)
            conn.commit()
            print("✓ Đã xóa unique constraint cũ")
        
        # Kiểm tra unique constraint mới
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.TABLE_CONSTRAINTS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'user_report_comparison_configs' 
            AND CONSTRAINT_NAME = 'uq_user_logger_comparison_config'
        """)
        new_constraint_exists = cursor.fetchone()[0] > 0
        
        if new_constraint_exists:
            print("✓ Unique constraint mới đã tồn tại")
        else:
            print("Đang tạo unique constraint mới...")
            # Tạo unique constraint mới với (user_id, logger_id)
            cursor.execute("""
                ALTER TABLE user_report_comparison_configs 
                ADD CONSTRAINT uq_user_logger_comparison_config 
                UNIQUE (user_id, logger_id)
            """)
            conn.commit()
            print("✓ Đã tạo unique constraint mới")
        
        # Kiểm tra và cập nhật index
        print("Đang kiểm tra index...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'user_report_comparison_configs' 
            AND INDEX_NAME = 'idx_user_comparison_config'
            AND COLUMN_NAME = 'logger_id'
        """)
        has_logger_in_index = cursor.fetchone()[0] > 0
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'user_report_comparison_configs' 
            AND INDEX_NAME = 'idx_user_comparison_config'
        """)
        index_exists = cursor.fetchone()[0] > 0
        
        if index_exists and not has_logger_in_index:
            print("Đang xóa index cũ...")
            cursor.execute("""
                DROP INDEX idx_user_comparison_config 
                ON user_report_comparison_configs
            """)
            conn.commit()
            print("✓ Đã xóa index cũ")
            
            print("Đang tạo index mới...")
            cursor.execute("""
                CREATE INDEX idx_user_comparison_config 
                ON user_report_comparison_configs (user_id, logger_id)
            """)
            conn.commit()
            print("✓ Đã tạo index mới")
        elif not index_exists:
            print("Đang tạo index...")
            cursor.execute("""
                CREATE INDEX idx_user_comparison_config 
                ON user_report_comparison_configs (user_id, logger_id)
            """)
            conn.commit()
            print("✓ Đã tạo index")
        else:
            print("✓ Index đã được cập nhật đúng")
        
        print("\n" + "="*60)
        print("✓ Migration hoàn tất thành công!")
        print("="*60)
        print("\nBây giờ mỗi datalogger sẽ có comparison config riêng.")
        print("Config cũ (nếu có) sẽ được áp dụng cho logger_id='all'")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Lỗi trong quá trình migration: {e}")
        print("Vui lòng kiểm tra lại database connection và quyền truy cập.")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("="*60)
    print("MIGRATION: Thêm cột logger_id vào comparison config")
    print("="*60)
    print("\nScript này sẽ:")
    print("1. Thêm cột logger_id vào bảng user_report_comparison_configs")
    print("2. Cập nhật unique constraint để bao gồm (user_id, logger_id)")
    print("3. Cập nhật index để tối ưu query")
    print("\n" + "="*60 + "\n")
    
    response = input("Bạn có muốn tiếp tục? (y/n): ")
    if response.lower() == 'y':
        migrate_comparison_config_table()
    else:
        print("Migration đã bị hủy.")
