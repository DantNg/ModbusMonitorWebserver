#!/usr/bin/env python3
"""
Datalogger Process - Handles data logging to database/files
Collects data from Modbus workers and stores it
Usage: python run_datalogger.py --interval 60 --database --csv
"""

import sys
import os
import argparse
import time
import threading
import json
from datetime import datetime
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    parser = argparse.ArgumentParser(description='Datalogger Process')
    parser.add_argument('--interval', type=int, default=60, help='Logging interval in seconds (default: 60)')
    parser.add_argument('--database', action='store_true', help='Enable database logging')
    parser.add_argument('--csv', action='store_true', help='Enable CSV file logging')
    parser.add_argument('--json', action='store_true', help='Enable JSON file logging')
    parser.add_argument('--output-dir', default='./logs', help='Output directory for files (default: ./logs)')
    parser.add_argument('--max-records', type=int, default=10000, help='Max records per file (default: 10000)')
    
    args = parser.parse_args()
    
    print(f"📊 Starting Datalogger Process")
    print(f"⏱️  Interval: {args.interval}s")
    print(f"🗄️  Database: {'✅' if args.database else '❌'}")
    print(f"📁 CSV: {'✅' if args.csv else '❌'}")
    print(f"📄 JSON: {'✅' if args.json else '❌'}")
    print(f"📂 Output: {args.output_dir}")
    print("-" * 50)
    
    if not any([args.database, args.csv, args.json]):
        print("⚠️  No output formats selected! Enable at least one: --database, --csv, or --json")
        return
    
    try:
        # Import after adding path
        from shared.database_manager import DatabaseManager
        from shared.data_collector import DataCollector
        from shared.file_logger import FileLogger
        
        # Initialize components
        db_manager = DatabaseManager() if args.database else None
        file_logger = FileLogger(args.output_dir) if (args.csv or args.json) else None
        data_collector = DataCollector()
        
        # Create output directory
        if file_logger and not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)
            print(f"📁 Created output directory: {args.output_dir}")
        
        print("🚀 Starting data collection...")
        
        record_count = 0
        current_file_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        while True:
            try:
                # Collect data from all active workers
                collected_data = data_collector.collect_all_data()
                
                if collected_data:
                    timestamp = datetime.now()
                    
                    # Database logging
                    if db_manager:
                        try:
                            for device_id, device_data in collected_data.items():
                                for tag_name, value in device_data['tags'].items():
                                    db_manager.execute_query(
                                        """INSERT INTO datalog (timestamp, device_id, tag_name, value, worker_id) 
                                           VALUES (?, ?, ?, ?, ?)""",
                                        (timestamp, device_id, tag_name, value, device_data.get('worker_id', 'unknown'))
                                    )
                            print(f"💾 Database: Logged {len(collected_data)} devices at {timestamp.strftime('%H:%M:%S')}")
                        except Exception as e:
                            print(f"❌ Database error: {e}")
                    
                    # File logging
                    if file_logger:
                        try:
                            # Check if need new file (max records reached)
                            if record_count >= args.max_records:
                                current_file_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
                                record_count = 0
                                print(f"🔄 Starting new log files: {current_file_suffix}")
                            
                            # CSV logging
                            if args.csv:
                                csv_data = []
                                for device_id, device_data in collected_data.items():
                                    for tag_name, value in device_data['tags'].items():
                                        csv_data.append({
                                            'timestamp': timestamp.isoformat(),
                                            'device_id': device_id,
                                            'device_name': device_data.get('name', 'Unknown'),
                                            'tag_name': tag_name,
                                            'value': value,
                                            'worker_id': device_data.get('worker_id', 'unknown')
                                        })
                                
                                csv_file = os.path.join(args.output_dir, f"datalog_{current_file_suffix}.csv")
                                file_logger.write_csv(csv_file, csv_data)
                            
                            # JSON logging
                            if args.json:
                                json_data = {
                                    'timestamp': timestamp.isoformat(),
                                    'devices': collected_data
                                }
                                json_file = os.path.join(args.output_dir, f"datalog_{current_file_suffix}.json")
                                file_logger.append_json(json_file, json_data)
                            
                            record_count += len(collected_data)
                            print(f"📁 Files: Logged {len(collected_data)} devices at {timestamp.strftime('%H:%M:%S')}")
                            
                        except Exception as e:
                            print(f"❌ File logging error: {e}")
                
                else:
                    print(f"🔍 No data collected at {datetime.now().strftime('%H:%M:%S')}")
                
            except Exception as e:
                print(f"❌ Collection error: {e}")
                import traceback
                traceback.print_exc()
            
            # Wait for next interval
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\n🛑 Datalogger stopped by user")
    except Exception as e:
        print(f"❌ Datalogger error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()