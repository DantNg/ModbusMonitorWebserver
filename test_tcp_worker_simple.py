#!/usr/bin/env python3
"""
Simple TCP Worker Test - Tests TCP worker without database dependency
Usage: python test_tcp_worker_simple.py 192.168.1.100 502
"""

import sys
import os
import argparse
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    parser = argparse.ArgumentParser(description='Simple TCP Worker Test')
    parser.add_argument('host', help='TCP host (e.g., 192.168.1.100)')
    parser.add_argument('port', type=int, help='TCP port (e.g., 502)')
    parser.add_argument('--timeout', type=int, default=5, help='Connection timeout (default: 5s)')
    
    args = parser.parse_args()
    
    print(f"🌐 Testing TCP Worker Process")
    print(f"🔗 Target: {args.host}:{args.port}")
    print(f"⏱️  Timeout: {args.timeout}s")
    print("-" * 50)
    
    try:
        from workers.tcp_worker import TCPWorker
        from shared.config import DeviceConfig, TagConfig
        
        # Create test device and tags
        test_device = DeviceConfig(
            id=1, name="Test Device", protocol="ModbusTCP", 
            host=args.host, port=args.port, unit_id=1,
            timeout_ms=args.timeout * 1000, read_interval_ms=1000,
            default_function_code=3, byte_order='big', word_order='big'
        )
        
        test_tags = [
            TagConfig(
                id=1, device_id=1, name="Temperature", address=1, 
                function_code=3, data_type="Word", scale_factor=1.0, offset=0.0
            ),
            TagConfig(
                id=2, device_id=1, name="Pressure", address=2,
                function_code=3, data_type="Word", scale_factor=1.0, offset=0.0
            )
        ]
        
        print(f"📋 Created test device: {test_device.name}")
        print(f"🏷️  Created {len(test_tags)} test tags")
        
        # Create TCP worker
        worker = TCPWorker(
            worker_id=f"TEST_TCP_{args.host}_{args.port}",
            host=args.host,
            port=args.port,
            timeout=args.timeout,
            devices=[test_device],
            tags=test_tags
        )
        
        print("🚀 Starting TCP worker test...")
        result = worker.start()
        
        if result:
            print("✅ TCP Worker started successfully")
            print("🔄 Running for 10 seconds...")
            
            # Let it run for 10 seconds
            time.sleep(10)
            
            print("🛑 Stopping TCP Worker...")
            worker.stop()
            print("✅ Test completed successfully")
        else:
            print("❌ TCP Worker failed to start")
            
    except KeyboardInterrupt:
        print("\n🛑 Test stopped by user")
        if 'worker' in locals():
            worker.stop()
    except Exception as e:
        print(f"❌ Test error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()