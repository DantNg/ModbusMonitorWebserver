"""
Data Collector - Collects data from various sources for datalogger and alarm processes
"""

import time
from datetime import datetime
from typing import Dict, Any
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

class DataCollector:
    def __init__(self):
        self.debug = True
    
    def collect_all_data(self) -> Dict[int, Dict[str, Any]]:
        """
        Collect data from all active workers/sources
        Returns: {device_id: {'name': device_name, 'worker_id': worker_id, 'tags': {tag_name: value}}}
        """
        try:
            # In a real implementation, this would:
            # 1. Connect to shared memory/queue where workers store data
            # 2. Query database for latest tag values
            # 3. Collect from all active data sources
            
            # For now, simulate collecting from database
            from shared.database_manager import DatabaseManager
            
            db_manager = DatabaseManager()
            
            # Get all devices
            devices = db_manager.execute_query("SELECT id, name FROM devices WHERE is_online = 1")
            
            collected_data = {}
            
            for device_row in devices:
                device_id = device_row[0]
                device_name = device_row[1]
                
                # Get tags for this device
                tags = db_manager.execute_query(
                    "SELECT id, name FROM tags WHERE device_id = ?", 
                    (device_id,)
                )
                
                device_data = {
                    'name': device_name,
                    'worker_id': f'worker_{device_id}',
                    'tags': {}
                }
                
                # Get latest values for each tag
                for tag_row in tags:
                    tag_id = tag_row[0]
                    tag_name = tag_row[1]
                    
                    # Get latest value from tag_latest_values table
                    latest_result = db_manager.execute_query(
                        "SELECT value, ts FROM tag_latest_values WHERE tag_id = ?",
                        (tag_id,)
                    )
                    
                    if latest_result:
                        value = latest_result[0][0]
                        device_data['tags'][tag_name] = value
                
                if device_data['tags']:  # Only include devices with data
                    collected_data[device_id] = device_data
            
            if self.debug and collected_data:
                print(f"📊 Collected data from {len(collected_data)} devices")
            
            return collected_data
            
        except Exception as e:
            print(f"❌ Data collection error: {e}")
            return {}
    
    def collect_device_data(self, device_id: int) -> Dict[str, Any]:
        """Collect data for a specific device"""
        all_data = self.collect_all_data()
        return all_data.get(device_id, {})
    
    def collect_tag_data(self, tag_id: int) -> Any:
        """Collect data for a specific tag"""
        try:
            from shared.database_manager import DatabaseManager
            
            db_manager = DatabaseManager()
            result = db_manager.execute_query(
                "SELECT value FROM tag_latest_values WHERE tag_id = ?",
                (tag_id,)
            )
            
            return result[0][0] if result else None
            
        except Exception as e:
            print(f"❌ Tag data collection error: {e}")
            return None