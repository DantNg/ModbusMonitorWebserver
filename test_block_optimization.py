#!/usr/bin/env python3
"""
Test TCP Worker với Block Optimization
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from workers.tcp_worker import TCPWorker
from shared.config import DeviceConfig, TagConfig

def test_block_optimization():
    print("🧪 Testing TCP Worker Block Optimization")
    print("-" * 50)
    
    # Create test device
    test_device = DeviceConfig(
        id=1, name="Test PLC", protocol="ModbusTCP", 
        host="127.0.0.1", port=502, unit_id=1,
        timeout_ms=5000, read_interval_ms=1000,
        default_function_code=3, byte_order='big', word_order='AB'
    )
    
    # Create test tags with different addresses (will be read as block)
    test_tags = [
        TagConfig(id=1, device_id=1, name="Temperature", address=40001, 
                 function_code=3, data_type="Word", scale_factor=0.1, offset=0.0),
        TagConfig(id=2, device_id=1, name="Pressure", address=40002,
                 function_code=3, data_type="Word", scale_factor=1.0, offset=0.0),
        TagConfig(id=3, device_id=1, name="Flow", address=40003,
                 function_code=3, data_type="Word", scale_factor=0.01, offset=0.0),
        TagConfig(id=4, device_id=1, name="Level", address=40005,  # Gap in addresses
                 function_code=3, data_type="Word", scale_factor=1.0, offset=0.0),
        TagConfig(id=5, device_id=1, name="Status", address=10001,  # Different function code
                 function_code=1, data_type="Bit", scale_factor=1.0, offset=0.0),
    ]
    
    print(f"📋 Test Device: {test_device.name}")
    print("🏷️  Test Tags:")
    for tag in test_tags:
        print(f"   • {tag.name}: FC{tag.function_code} @ {tag.address}")
    
    # Create worker (will run in simulation mode)
    worker = TCPWorker(
        worker_id="TEST_BLOCK_OPT",
        host="127.0.0.1",
        port=502,
        timeout=5,
        devices=[test_device],
        tags=test_tags
    )
    
    print("\n🚀 Starting worker...")
    worker.start()
    
    # Let it run for a few cycles
    import time
    time.sleep(5)
    
    print("\n🛑 Stopping worker...")
    worker.stop()
    
    print("\n✅ Test completed!")
    print("\n📊 Expected behavior:")
    print("   • Tags FC3 @ 40001-40005 should be read as 1 block (5 registers)")
    print("   • Tag FC1 @ 10001 should be read separately")
    print("   • Shows block read debug info with address mapping")

if __name__ == "__main__":
    test_block_optimization()