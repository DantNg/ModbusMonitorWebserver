#!/usr/bin/env python3
"""
TCP Worker Process - Handles Modbus TCP communication  
Run this for each TCP connection (host:port)
Usage: python run_tcp_worker.py 127.0.0.1 502
"""

import sys
import os
import argparse
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    parser = argparse.ArgumentParser(description='TCP Worker Process')
    parser.add_argument('host', help='TCP host (e.g., 192.168.1.100)')
    parser.add_argument('port', type=int, help='TCP port (e.g., 502)')
    parser.add_argument('--timeout', type=int, default=5, help='Connection timeout (default: 5s)')
    
    args = parser.parse_args()
    
    print(f"🌐 Starting TCP Worker Process")
    print(f"🔗 Target: {args.host}:{args.port}")
    print(f"⏱️  Timeout: {args.timeout}s")
    print("-" * 50)
    
    try:
        # Import after adding path to avoid import errors
        from workers.tcp_worker import TCPWorker
        from shared.database_manager import DatabaseManager
        from shared.config import DeviceConfig, TagConfig
        
        # Initialize database
        db_manager = DatabaseManager()
        
        # Load devices for this TCP connection
        query = """
        SELECT id, name, protocol, host, port, serial_port, baudrate, bytesize, 
               parity, stopbits, unit_id, timeout_ms, read_interval_ms, 
               default_function_code, byte_order 
        FROM devices 
        WHERE protocol IN ('ModbusTCP', 'TCP') AND host = :host AND port = :port
        """
        
        device_rows = db_manager.execute_query(query, {"host": args.host, "port": args.port})
        
        if not device_rows:
            print(f"⚠️  No TCP devices found for {args.host}:{args.port}")
            print("💡 Add TCP devices in the webapp first")
            return
        
        devices = []
        tags = []
        
        for row in device_rows:
            device = DeviceConfig(
                id=row[0], name=row[1], protocol=row[2], host=row[3], port=row[4],
                serial_port=row[5], baudrate=row[6], bytesize=row[7], parity=row[8],
                stopbits=row[9], unit_id=row[10], timeout_ms=row[11], 
                read_interval_ms=row[12], default_function_code=row[13],
                byte_order=row[14], word_order='AB'  # Default word order
            )
            devices.append(device)
            
            # Load tags for this device
            tag_query = """
            SELECT id, device_id, name, address, function_code, datatype, 
                   scale, offset, description 
            FROM tags 
            WHERE device_id = :device_id
            """
            tag_rows = db_manager.execute_query(tag_query, {"device_id": device.id})
            
            for tag_row in tag_rows:
                tag = TagConfig(
                    id=tag_row[0], device_id=tag_row[1], name=tag_row[2],
                    address=tag_row[3], function_code=tag_row[4], data_type=tag_row[5],
                    scale_factor=tag_row[6], offset=tag_row[7], description=tag_row[8],
                    is_readonly=True  # Default to readonly
                )
                tags.append(tag)
        
        print(f"📋 Loaded {len(devices)} devices, {len(tags)} tags")
        for device in devices:
            print(f"   🖥️  {device.name} (Unit ID: {device.unit_id})")
        
        # Create TCP worker
        worker = TCPWorker(
            worker_id=f"TCP_{args.host}_{args.port}",
            host=args.host,
            port=args.port,
            timeout=args.timeout,
            devices=devices,
            tags=tags
        )
        
        print("🚀 Starting TCP communication...")
        worker.start()
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 TCP Worker stopped by user")
        worker.stop()
    except Exception as e:
        print(f"❌ TCP Worker error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()