"""
Database Manager - Handles database connections and operations for independent processes
"""

import json
import threading
import os
import sys
from contextlib import contextmanager
from sqlalchemy import create_engine, text, update
from sqlalchemy.engine import Engine
from webapp.modbus_monitor.database.db import update_device_row

class DatabaseManager:
    def __init__(self, config_path=None):
        self.local = threading.local()
        # Resolve config path robustly
        self.config_path = self._resolve_config_path(config_path)
        self._ensure_connection()

    def _resolve_config_path(self, override_path=None):
        if override_path and os.path.exists(override_path):
            return override_path
        env_path = os.environ.get('SMTP_CONFIG_PATH')
        if env_path and os.path.exists(env_path):
            return env_path
        meipass = getattr(sys, '_MEIPASS', None)
        try:
            exe_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else None
        except Exception:
            exe_dir = None
        candidates = []
        if exe_dir:
            candidates.append(os.path.join(exe_dir, 'config', 'SMTP_config.json'))
            candidates.append(os.path.join(exe_dir, 'SMTP_config.json'))
        # CWD
        cwd = os.getcwd()
        candidates.append(os.path.join(cwd, 'config', 'SMTP_config.json'))
        candidates.append(os.path.join(cwd, 'SMTP_config.json'))
        # Project root (when running from source)
        proj_root = os.path.dirname(os.path.dirname(__file__))
        candidates.append(os.path.join(proj_root, 'config', 'SMTP_config.json'))
        if meipass:
            candidates.append(os.path.join(meipass, 'config', 'SMTP_config.json'))
        for p in candidates:
            if os.path.exists(p):
                return p
        # As last resort, return the typical project path
        return os.path.join(proj_root, 'config', 'SMTP_config.json')
    
    def _ensure_connection(self):
        """Ensure thread-local connection exists"""
        if not hasattr(self.local, 'engine'):
            try:
                # Load database configuration from SMTP_config.json
                with open(self.config_path) as config_file:
                    config = json.load(config_file)
                
                uri = config.get("MYSQL_URI", "mysql+pymysql://root:123456@localhost:3306/modbus_monitor_db")
                pool_size = int(config.get("POOL_SIZE", "8"))
                
                self.local.engine = create_engine(
                    uri,
                    pool_pre_ping=True,
                    pool_size=pool_size,
                    max_overflow=pool_size,
                    future=True,
                )
                
                print(f"📊 Database connected: {uri.split('@')[-1] if '@' in uri else uri}")
                
            except Exception as e:
                print(f"❌ Database connection error: {e}")
                # Fallback to SQLite if MySQL fails
                import sqlite3
                # Store fallback DB next to executable when frozen; else in project root
                try:
                    base_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.dirname(__file__))
                except Exception:
                    base_dir = os.path.dirname(os.path.dirname(__file__))
                self.local.fallback_db = os.path.join(base_dir, 'modbus_monitor.db')
                print(f"⚠️ Falling back to SQLite: {self.local.fallback_db}")
    
    @contextmanager
    def get_connection(self):
        """Get thread-safe database connection"""
        self._ensure_connection()
        try:
            if hasattr(self.local, 'engine'):
                # Use SQLAlchemy engine
                with self.local.engine.connect() as conn:
                    yield conn
            else:
                # Fallback to SQLite
                import sqlite3
                conn = sqlite3.connect(self.local.fallback_db, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                try:
                    yield conn
                except Exception as e:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
        except Exception as e:
            raise
    
    def execute_query(self, query, params=None):
        """Execute SELECT query and return results"""
        try:
            with self.get_connection() as conn:
                if hasattr(self.local, 'engine'):
                    # SQLAlchemy
                    if params:
                        result = conn.execute(text(query), params)
                    else:
                        result = conn.execute(text(query))
                    return result.fetchall()
                else:
                    # SQLite fallback
                    cursor = conn.cursor()
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    return cursor.fetchall()
        except Exception as e:
            print(f"❌ Query execution error: {e}")
            return []
    
    def execute_update(self, query, params=None):
        """Execute INSERT/UPDATE/DELETE query"""
        try:
            with self.get_connection() as conn:
                if hasattr(self.local, 'engine'):
                    # SQLAlchemy
                    if params:
                        result = conn.execute(text(query), params)
                    else:
                        result = conn.execute(text(query))
                    conn.commit()
                    return result.rowcount
                else:
                    # SQLite fallback
                    cursor = conn.cursor()
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    conn.commit()
                    return cursor.rowcount
        except Exception as e:
            print(f"❌ Update execution error: {e}")
            return 0
    
    def execute_many(self, query, params_list):
        """Execute query with multiple parameter sets"""
        try:
            with self.get_connection() as conn:
                if hasattr(self.local, 'engine'):
                    # SQLAlchemy
                    for params in params_list:
                        conn.execute(text(query), params)
                    conn.commit()
                    return len(params_list)
                else:
                    # SQLite fallback
                    cursor = conn.cursor()
                    cursor.executemany(query, params_list)
                    conn.commit()
                    return cursor.rowcount
        except Exception as e:
            print(f"❌ Batch execution error: {e}")
            return 0
    
    def update_tag_latest_value(self, tag_id, value, timestamp):
        """Update latest value for a tag in tag_latest_values table"""
        try:
            # Convert timestamp to string format for database
            timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S') if hasattr(timestamp, 'strftime') else str(timestamp)
            
            # Use UPSERT logic for tag_latest_values table
            query = """
                INSERT INTO tag_latest_values (tag_id, value, ts, updated_at) 
                VALUES (:tag_id, :value, :timestamp, :updated_at)
                ON DUPLICATE KEY UPDATE 
                    value = VALUES(value), 
                    ts = VALUES(ts), 
                    updated_at = VALUES(updated_at)
            """
            params = {
                'tag_id': tag_id,
                'value': value,
                'timestamp': timestamp_str,
                'updated_at': timestamp_str
            }
            
            result = self.execute_update(query, params)
            return result >= 0  # INSERT or UPDATE both return >= 0
            
        except Exception as e:
            print(f"Error updating tag latest value {tag_id}: {e}")
            # Fallback: try simple INSERT (for first time)
            try:
                fallback_query = """
                    INSERT IGNORE INTO tag_latest_values (tag_id, value, ts, updated_at) 
                    VALUES (:tag_id, :value, :timestamp, :updated_at)
                """
                result = self.execute_update(fallback_query, params)
                return result > 0
            except Exception as e2:
                print(f"Error with fallback insert for tag {tag_id}: {e2}")
                return False
    
    def update_device_row(self, device_id, data: dict):
        """Update a device."""
        print("Called update_device_row in DatabaseManager")
        return update_device_row(device_id, data)

    def close(self):
        """Close database connection"""
        if hasattr(self.local, 'engine'):
            self.local.engine.dispose()
            del self.local.engine