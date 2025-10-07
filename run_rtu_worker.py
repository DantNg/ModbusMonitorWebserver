#!/usr/bin/env python3
"""
RTU Worker Process - Handles Modbus RTU communication over serial
Run this for each RTU connection (serial port)
Usage: python run_rtu_worker.py COM3 --baudrate 9600
"""

import sys
import os
import argparse
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    parser = argparse.ArgumentParser(description='RTU Worker Process')
    parser.add_argument('serial_port', help='Serial port (e.g., COM3, /dev/ttyUSB0)')
    parser.add_argument('--baudrate', type=int, default=9600, help='Baudrate (default: 9600)')
    parser.add_argument('--bytesize', type=int, default=8, help='Bytesize (default: 8)')
    parser.add_argument('--parity', default='N', help='Parity (default: N)')
    parser.add_argument('--stopbits', type=int, default=1, help='Stopbits (default: 1)')
    
    args = parser.parse_args()
    
    print(f"📻 Starting RTU Worker Process")
    print(f"🔗 Serial Port: {args.serial_port}")
    print(f"⚡ Baudrate: {args.baudrate}")
    print(f"📊 Config: {args.bytesize}-{args.parity}-{args.stopbits}")
    print("-" * 50)
    
    try:
        # Import after adding path to avoid import errors
        from workers.rtu_worker import RTUWorker
        from shared.database_manager import DatabaseManager
        from shared.config import DeviceConfig, TagConfig
        
        # Initialize database
        db_manager = DatabaseManager()
        
        # Load devices for this RTU connection
        query = """
        SELECT id, name, protocol, host, port, serial_port, baudrate, bytesize, 
               parity, stopbits, unit_id, timeout_ms, read_interval_ms, 
               default_function_code, byte_order 
        FROM devices 
        WHERE protocol IN ('ModbusRTU', 'RTU') AND serial_port = :serial_port
        """
        
        device_rows = db_manager.execute_query(query, {"serial_port": args.serial_port})
        
        if not device_rows:
            print(f"⚠️  No RTU devices found for {args.serial_port}")
            print("💡 Add RTU devices in the webapp first")
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
            print(f"   📻 {device.name} (Unit ID: {device.unit_id})")
        
        # Create RTU worker
        worker = RTUWorker(
            worker_id=f"RTU_{args.serial_port}",
            serial_port=args.serial_port,
            baudrate=args.baudrate,
            timeout=5,
            devices=devices,
            tags=tags
        )
        
        print("🚀 Starting RTU communication...")
        worker.start()
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 RTU Worker stopped by user")
        worker.stop()
    except Exception as e:
        print(f"❌ RTU Worker error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()