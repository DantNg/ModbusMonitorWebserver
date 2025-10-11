#!/usr/bin/env python3
import sys
import os

# Add webapp to path
WEBAPP_DIR = os.path.join('.', 'webapp')
sys.path.insert(0, WEBAPP_DIR)

from modbus_monitor.database import db

# Test database connectivity and check devices
print("=== Testing Database Connectivity ===")
try:
    devices = db.list_devices()
    print(f"Found {len(devices)} devices in database:")
    
    for device in devices:
        print(f"  - ID: {device['id']}, Name: {device['name']}")
        print(f"    Protocol: {device['protocol']}")
        print(f"    Host: {device.get('host', 'N/A')}, Port: {device.get('port', 'N/A')}")
        print(f"    Serial: {device.get('serial_port', 'N/A')}, Baudrate: {device.get('baudrate', 'N/A')}")
        print(f"    Is Online: {device.get('is_online', 'NULL')}")
        
        # Get tags for this device
        tags = db.list_tags(device['id'])
        print(f"    Tags: {len(tags)} tags")
        print("")
        
except Exception as e:
    print(f"Error accessing database: {e}")

print("\n=== Testing Worker Generation ===")
try:
    workers = db.get_auto_start_workers()
    print(f'Found {len(workers)} auto-generated workers:')

    for w in workers:
        print(f'  - {w["worker_id"]}: {w["worker_type"]} ({len(w["devices"])} devices, {len(w["tags"])} tags)')
        if w['worker_type'] == 'tcp':
            print(f'    TCP: {w["host"]}:{w["port"]}')
        elif w['worker_type'] == 'rtu':
            print(f'    RTU: {w["serial_port"]}@{w["baudrate"]}')
        
        # Show devices in this worker
        for device in w["devices"]:
            print(f'      Device: {device["name"]} (ID: {device["id"]})')
            
except Exception as e:
    print(f"Error generating workers: {e}")